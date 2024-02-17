package infrastructure

import (
	"log"
	"main/utils"
	"main/worker/domain"
	"strconv"
	"time"
)

type ProcessingStation struct {
	processingQueue             chan domain.ActorManager
	completedActorManagersQueue chan<- domain.ActorManager

	processingSlotsCount        int
	maxMessageProcessingRetries int

	taskDao                    domain.TaskDao
	notificationStorageFactory domain.NotificationStorageFactory
	retryFactory               func() *utils.Retrier[struct{}]
}

func NewProcessingStation(
	processingQueue chan domain.ActorManager, completedActorManagersQueue chan<- domain.ActorManager,
	processingSlotsCount int, maxMessageProcessingRetries int,
	taskDao domain.TaskDao,
	notificationStorageFactory domain.NotificationStorageFactory,
	retryFactory func() *utils.Retrier[struct{}]) *ProcessingStation {
	return &ProcessingStation{
		processingQueue:             processingQueue,
		completedActorManagersQueue: completedActorManagersQueue,
		processingSlotsCount:        processingSlotsCount,
		maxMessageProcessingRetries: maxMessageProcessingRetries,
		taskDao:                     taskDao,
		notificationStorageFactory:  notificationStorageFactory,
		retryFactory:                retryFactory,
	}
}

func (ps *ProcessingStation) Start() {
	for i := range ps.processingSlotsCount {
		go processSlot(ps.processingQueue, ps.completedActorManagersQueue, ps.taskDao, ps.notificationStorageFactory.BuildNotificationStorage(strconv.Itoa(i)), ps.maxMessageProcessingRetries, ps.retryFactory())
	}
}

func processSlot(
	readyQueue chan domain.ActorManager,
	completedActorManagersQueue chan<- domain.ActorManager,
	taskDao domain.TaskDao,
	notificationStorage domain.NotificationStorage,
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

		notifications := notificationStorage.GetAllNotifications()

		//map-reduce to flush notifications
		type notificationResult struct {
			phyPartitionId  domain.PhysicalPartitionId
			hasBeenNotified bool
		}

		inputQueue := make(chan domain.Notification, len(notifications))
		outputQueue := make(chan notificationResult, len(notifications))
		maxConcurrentNotifiers := min(20, len(notifications))

		//producer
		go func() {
			for _, notification := range notifications {
				inputQueue <- notification
			}
			close(inputQueue)
		}()

		//mappers
		for range maxConcurrentNotifiers {
			go func() {
				for notification := range inputQueue {
					success := tryActivatePhyPartitionIfPassivated(notification.PhyPartitionId, taskDao)
					outputQueue <- notificationResult{phyPartitionId: notification.PhyPartitionId, hasBeenNotified: success}
				}
			}()
		}

		var successfullyProcessedNotifications []domain.Notification

		for range len(notifications) {
			result := <-outputQueue
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

		completedActorManagersQueue <- actorManager
	}

	err := notificationStorage.Close()
	if err != nil {
		return
	}

}

// if the function returns true the actor is active at the end of the call
func tryActivatePhyPartitionIfPassivated(phyPartitionId domain.PhysicalPartitionId, taskDao domain.TaskDao) bool {
	taskStatus, err := taskDao.GetTaskStatus(phyPartitionId)
	if err != nil {
		return false
	}

	if taskStatus.IsSealed {
		return false
	}

	if !taskStatus.IsActorPassivated {
		return true
	}

	// the actor was passivated, so we need to activate it
	err = taskDao.AddTask(phyPartitionId, time.Now())

	if err != nil {
		log.Printf("could not add task: %v", err)
	}
	return err != nil
}
