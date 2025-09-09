package infrastructure

import (
	"log"
	"main/utils"
	"main/worker/domain"
	"main/worker/notification"
	"strconv"
	"sync"
	"time"
)

type ProcessingStation struct {
	processingQueue             chan domain.ActorManager
	completedActorManagersQueue chan<- domain.ActorManager

	processingSlotsCount        int
	maxMessageProcessingRetries int

	taskDao                    domain.TaskDao
	notificationStorageFactory domain.NotificationStorageFactory
	amqp                       bool
	retryFactory               func() *utils.Retrier[struct{}]
}

func NewProcessingStation(
	processingQueue chan domain.ActorManager, completedActorManagersQueue chan<- domain.ActorManager,
	processingSlotsCount int, maxMessageProcessingRetries int,
	taskDao domain.TaskDao,
	notificationStorageFactory domain.NotificationStorageFactory,
	amqp bool,
	retryFactory func() *utils.Retrier[struct{}]) *ProcessingStation {
	return &ProcessingStation{
		processingQueue:             processingQueue,
		completedActorManagersQueue: completedActorManagersQueue,
		processingSlotsCount:        processingSlotsCount,
		maxMessageProcessingRetries: maxMessageProcessingRetries,
		taskDao:                     taskDao,
		notificationStorageFactory:  notificationStorageFactory,
		amqp:                        amqp,
		retryFactory:                retryFactory,
	}
}

func (ps *ProcessingStation) Start() {
	activateWorkers := make(chan string, 64)

	if ps.amqp {
		notifier, err := notification.NewMQNotifier()
		if err != nil {
			log.Fatalf("Couldn't start AMQP Notifier")
		}

		go amqpNotifier(notifier, activateWorkers)
	}

	for i := range ps.processingSlotsCount {
		go processSlot(
			ps.processingQueue,
			ps.completedActorManagersQueue,
			ps.taskDao,
			ps.notificationStorageFactory.BuildNotificationStorage(strconv.Itoa(i)),
			activateWorkers,
			ps.maxMessageProcessingRetries,
			ps.retryFactory(),
		)
	}
}

func processSlot(
	readyQueue chan domain.ActorManager,
	completedActorManagersQueue chan<- domain.ActorManager,
	taskDao domain.TaskDao,
	notificationStorage domain.NotificationStorage,
	activateWorkers chan<- string,
	maxMessageProcessingRetries int,
	retrier *utils.Retrier[struct{}]) {

	for actorManager := range readyQueue {
		consecutiveRetries := 0
		for !actorManager.IsQueueEmpty() {
			//messageProcessingStartTime := time.Now()
			recipientsIds, err := actorManager.PrepareMessageProcessing()

			if err != nil {
				log.Printf("Actor failed to process message: %v\n", err)
				consecutiveRetries++
			} else {
				//notificationLoggingStartTime := time.Now()
				recipientsIds.ForEach(func(recipientId domain.PhysicalPartitionId) bool {
					err = notificationStorage.AddNotification(domain.Notification{PhyPartitionId: recipientId})
					return err != nil
				})
				//log.Printf("Logging notification delay [%v]: %v\n", actorManager.GetActorId(), time.Since(notificationLoggingStartTime))
				if err != nil { //failed to log a notification
					log.Printf("Notification loggin failed: %v\n", err)
					consecutiveRetries++
					actorManager.ForceMessageProcessingRollback()
				} else {
					transactionStartTime := time.Now()
					_, transactionErr := retrier.DoWithReturn(func() (struct{}, error) {
						return struct{}{}, actorManager.CommitMessageProcessing()
					})
					log.Printf("Transaction delay [%v]: %v\n", actorManager.GetActorId(), time.Since(transactionStartTime))
					if transactionErr != nil { //failed to commit transaction
						log.Printf("Transaction failed: %v\n", err)
						consecutiveRetries++
					} else {
						consecutiveRetries = 0
					}
				}
			}

			if consecutiveRetries > maxMessageProcessingRetries {
				log.Fatalf("too many retries for actor %v", actorManager.GetActorId())
			}

		}

		activateNotifications(notificationStorage, taskDao, activateWorkers)

		completedActorManagersQueue <- actorManager
	}

	err := notificationStorage.Close()
	if err != nil {
		return
	}

}

func amqpNotifier(notifier *notification.MQNotifier, activateWorkers <-chan string) {
	for w := range activateWorkers {
		notifier.Notify(w)
	}
}

func activateNotifications(notificationStorage domain.NotificationStorage, taskDao domain.TaskDao, activateWorkers chan<- string) {
	notifications := notificationStorage.GetAllNotifications()

	//map-reduce to flush notifications
	type notificationResult struct {
		phyPartitionId  domain.PhysicalPartitionId
		hasBeenNotified bool
	}

	inputQueue := make(chan domain.Notification, len(notifications))
	outputQueue := make(chan notificationResult, len(notifications))
	workers := make(chan string, len(notifications))
	maxConcurrentNotifiers := min(20, len(notifications))
	var wg sync.WaitGroup

	for _, notification := range notifications {
		inputQueue <- notification
	}
	close(inputQueue)

	for range maxConcurrentNotifiers {
		if len(inputQueue) == 0 {
			break
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			for notification := range inputQueue {
				success, workerId := tryActivatePhyPartition(notification.PhyPartitionId, taskDao)
				outputQueue <- notificationResult{phyPartitionId: notification.PhyPartitionId, hasBeenNotified: success}

				if workerId != "" && workerId != "NULL" {
					workers <- workerId
				}
			}
		}()
	}

	wg.Wait()

	close(outputQueue)
	close(workers)

	var successfullyProcessedNotifications []domain.Notification

	for result := range outputQueue {
		if result.hasBeenNotified {
			successfullyProcessedNotifications = append(successfullyProcessedNotifications, domain.Notification{PhyPartitionId: result.phyPartitionId})
		}
	}

	//flushingNotificationsStartTime := time.Now()
	err := notificationStorage.RemoveAllNotifications(successfullyProcessedNotifications...)
	//log.Printf("Flushing notifications delay [%v]: %v\n", actorManager.GetActorId(), time.Since(flushingNotificationsStartTime))

	if err != nil {
		log.Printf("could not remove all notifications: %v\n", err)
	}

	uniqueWorkers := utils.NewMapSet[string]()
	for w := range workers {
		if !uniqueWorkers.Contains(w) {
			uniqueWorkers.Add(w)
			select { // non-blocking send
			case activateWorkers <- w:
			default:
			}
		}
	}
}

// if the function returns true the actor is active at the end of the call
func tryActivatePhyPartition(phyPartitionId domain.PhysicalPartitionId, taskDao domain.TaskDao) (bool, string) {
	taskStatus, err := taskDao.GetTaskStatus(phyPartitionId)
	if err != nil {
		return false, ""
	}

	if taskStatus.IsSealed {
		return false, taskStatus.WorkerId
	}

	if taskStatus.IsActive {
		return true, taskStatus.WorkerId
	}

	// the actor was passivated, so we need to activate it
	err = taskDao.AddTask(phyPartitionId, time.Now())

	if err != nil {
		log.Printf("could not add task: %v", err)
	}
	return err != nil, ""
}
