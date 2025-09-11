package infrastructure

import (
	"log"
	"main/utils"
	"main/worker/domain"
	"main/worker/dyndao"
	"main/worker/notification"
	"reflect"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

type Worker struct {
	periodicSignal        <-chan time.Time
	pollInboxSignal       <-chan time.Time
	parkingPeriodicSignal <-chan time.Time
	notifySignal          <-chan time.Time
	parkNotifySignal      <-chan time.Time

	workerId                                     string
	runId                                        string
	maxActorsCount                               int
	maximumConcurrentShardPullsCount             int
	maxConcurrentProcessingActors                int
	maxMessageProcessingRetries                  int
	maximumSubsequentEmptyPhyPartitionsPullCount int
	useBackoffStrategy                           bool
	idleMillisecondsToWaitBeforeParking          int64
	passivationIntervalMillis                    int64

	taskDao                    domain.TaskDao
	phyPartitionManagerFactory domain.PhysicalPartitionManagerFactory
	notificationStorageFactory domain.NotificationStorageFactory

	amqp           bool
	retrierFactory func() *utils.Retrier[struct{}]
}

func NewWorker(periodicSignal <-chan time.Time, pollInboxSignal <-chan time.Time, parkingSignal <-chan time.Time, notifySignal <-chan time.Time, parkNotifySignal <-chan time.Time, workerId string, runId string, maxActorsCount int, maximumConcurrentShardPullsCount int, maxConcurrentProcessingActors int, maxMessageProcessingRetries int, maximumSubsequentEmptyPhyPartitionsPullCount int, useBackoffStrategy bool, idleMillisecondsToWaitBeforeParking int64, passivationIntervalMillis int64, taskDao domain.TaskDao, phyPartitionManagerFactory domain.PhysicalPartitionManagerFactory, notificationStorageFactory domain.NotificationStorageFactory, retrierFactory func() *utils.Retrier[struct{}], amqp bool) *Worker {
	return &Worker{periodicSignal: periodicSignal, pollInboxSignal: pollInboxSignal, parkingPeriodicSignal: parkingSignal, notifySignal: notifySignal, parkNotifySignal: parkNotifySignal, workerId: workerId, runId: runId, maxActorsCount: maxActorsCount, maximumConcurrentShardPullsCount: maximumConcurrentShardPullsCount, maxConcurrentProcessingActors: maxConcurrentProcessingActors, maxMessageProcessingRetries: maxMessageProcessingRetries, maximumSubsequentEmptyPhyPartitionsPullCount: maximumSubsequentEmptyPhyPartitionsPullCount, useBackoffStrategy: useBackoffStrategy, idleMillisecondsToWaitBeforeParking: idleMillisecondsToWaitBeforeParking, passivationIntervalMillis: passivationIntervalMillis, taskDao: taskDao, phyPartitionManagerFactory: phyPartitionManagerFactory, notificationStorageFactory: notificationStorageFactory, retrierFactory: retrierFactory, amqp: amqp}
}

func (w *Worker) Run() {

	newPhyPartitionsQueue := make(chan []domain.PhysicalPartitionManager, 5)
	newPhyPartitionsFromParkingQueue := make(chan domain.PhysicalPartitionManager, 10)
	passivatedPhyPartitionsCountSignal := make(chan int, 100)
	completedActorManagersQueue := make(chan domain.ActorManager, w.maxConcurrentProcessingActors*2)
	processingQueue := make(chan domain.ActorManager, w.maxConcurrentProcessingActors*2)
	parkingQueue := make(chan domain.PhysicalPartitionManager, 10)
	pullRequestSignal := make(chan ShardPullRequest, 10)

	phyPartitionsStation := NewPhysicalPartitionStation(
		w.periodicSignal,
		w.pollInboxSignal,
		w.notifySignal,
		newPhyPartitionsQueue,
		newPhyPartitionsFromParkingQueue,
		passivatedPhyPartitionsCountSignal,
		completedActorManagersQueue,
		processingQueue,
		parkingQueue,
		pullRequestSignal,
		w.workerId,
		w.maximumConcurrentShardPullsCount,
		w.maxConcurrentProcessingActors,
		w.maxActorsCount/2,
		w.maximumSubsequentEmptyPhyPartitionsPullCount,
		w.useBackoffStrategy,
		w.idleMillisecondsToWaitBeforeParking,
	)

	pullingStation := NewPullingStation(
		pullRequestSignal,
		newPhyPartitionsQueue,
		w.taskDao,
		w.phyPartitionManagerFactory,
		w.workerId,
		w.runId)

	parkingStation := NewParkingStation(
		parkingQueue,
		w.parkingPeriodicSignal,
		w.parkNotifySignal,
		newPhyPartitionsFromParkingQueue,
		passivatedPhyPartitionsCountSignal,
		w.passivationIntervalMillis,
	)

	processingStation := NewProcessingStation(
		processingQueue,
		completedActorManagersQueue,
		w.maxConcurrentProcessingActors,
		w.maxMessageProcessingRetries,
		w.taskDao,
		w.notificationStorageFactory,
		w.amqp,
		w.retrierFactory,
	)

	pullingStation.Start()
	parkingStation.Start()
	processingStation.Start()
	phyPartitionsStation.Start()

	close(processingQueue)
}

type WorkerParameters struct {
	WorkerId                                     string
	RunId                                        string
	BaseClockSynchronizerUrl                     string
	Amqp                                         bool
	MaxActorsCount                               int
	MaximumConcurrentShardPullsCount             int
	MaxConcurrentProcessingActors                int
	MaxMessageProcessingRetries                  int
	MaximumSubsequentEmptyPhyPartitionsPullCount int
	UseBackoffStrategy                           bool
	IdleMillisecondsToWaitBeforeParking          int64
	PassivationIntervalMillis                    int64

	PeriodicTimerMillis    int64
	PollingTimerMillis     int64
	PassivatingTimerMillis int64
	RetryBehaviour         RetryBehaviourParams
}

type RetryBehaviourParams struct {
	IsEnabled                bool
	InitialDelayMilliseconds int64
	MaxDelayMilliseconds     int64
	MaxRetries               int
	JitterPercentage         float64
}

func NewWorkerParameters(workerId string, runId string, baseClockSynchronizerUrl string, maxActorsCount int, maximumConcurrentShardPullsCount int, maxConcurrentProcessingActors int, maxMessageProcessingRetries int, maximumSubsequentEmptyPhyPartitionsPullCount int, useBackoffStrategy bool, idleMillisecondsToWaitBeforeParking int64, periodicTimerMillis int64, pollingTimerMillis int64, passivatingTimerMillis int64, retryBehaviourParams RetryBehaviourParams) *WorkerParameters {
	return &WorkerParameters{WorkerId: workerId, RunId: runId, BaseClockSynchronizerUrl: baseClockSynchronizerUrl, MaxActorsCount: maxActorsCount, MaximumConcurrentShardPullsCount: maximumConcurrentShardPullsCount, MaxConcurrentProcessingActors: maxConcurrentProcessingActors, MaxMessageProcessingRetries: maxMessageProcessingRetries, MaximumSubsequentEmptyPhyPartitionsPullCount: maximumSubsequentEmptyPhyPartitionsPullCount, UseBackoffStrategy: useBackoffStrategy, IdleMillisecondsToWaitBeforeParking: idleMillisecondsToWaitBeforeParking, PeriodicTimerMillis: periodicTimerMillis, PollingTimerMillis: pollingTimerMillis, PassivatingTimerMillis: passivatingTimerMillis, RetryBehaviour: retryBehaviourParams}
}

func IsWorkerParametersValid(params *WorkerParameters) bool {
	return params.WorkerId != "" && params.RunId != "" &&
		params.MaxActorsCount > 0 &&
		params.MaximumConcurrentShardPullsCount > 0 &&
		params.MaxConcurrentProcessingActors > 0 &&
		params.MaxMessageProcessingRetries >= 0 &&
		params.MaximumSubsequentEmptyPhyPartitionsPullCount >= 0 &&
		params.IdleMillisecondsToWaitBeforeParking >= 0 &&
		params.PeriodicTimerMillis >= 0 &&
		params.PollingTimerMillis >= 0 &&
		params.PassivatingTimerMillis >= 0
}

func BuildNewWorker(params *WorkerParameters, client *dynamodb.Client, timestampCollectorFactory domain.TimestampCollectorFactory) *Worker {
	taskDao := dyndao.DynTaskDao{Client: client}
	entityLoader := domain.NewEntityLoader()
	entityLoader.RegisterType("MyActor", reflect.TypeOf(domain.MyActor{}))
	entityLoader.RegisterType("Hotel", reflect.TypeOf(domain.Hotel{}))
	entityLoader.RegisterType("WeekAvailability", reflect.TypeOf(domain.WeekAvailability{}))
	entityLoader.RegisterType("SimpleMessage", reflect.TypeOf(domain.SimpleMessage{}))
	entityLoader.RegisterType("BookingRequest", reflect.TypeOf(domain.BookingRequest{}))
	entityLoader.RegisterType("BookingResponse", reflect.TypeOf(domain.BookingResponse{}))
	entityLoader.RegisterType("User", reflect.TypeOf(domain.User{}))
	entityLoader.RegisterType("TransactionRequest", reflect.TypeOf(domain.TransactionRequest{}))
	entityLoader.RegisterType("TransactionResponse", reflect.TypeOf(domain.TransactionResponse{}))
	entityLoader.RegisterType("BankBranch", reflect.TypeOf(domain.BankBranch{}))
	entityLoader.RegisterType("Account", reflect.TypeOf(domain.Account{}))
	entityLoader.RegisterType("TravelAgency", reflect.TypeOf(domain.TravelAgency{}))
	entityLoader.RegisterType("DiscountRequest", reflect.TypeOf(domain.DiscountRequest{}))
	entityLoader.RegisterType("Journey", reflect.TypeOf(domain.Journey{}))
	entityLoader.RegisterType("AddressUpdateRequest", reflect.TypeOf(domain.AddressUpdateRequest{}))
	entityLoader.RegisterType("TravelBookingRequest", reflect.TypeOf(domain.TravelBookingRequest{}))
	entityLoader.RegisterType("TravelBookingReply", reflect.TypeOf(domain.TravelBookingReply{}))
	entityLoader.RegisterType("TravelAgent", reflect.TypeOf(domain.TravelAgent{}))
	entityLoader.RegisterType("SinkActor", reflect.TypeOf(domain.SinkActor{}))

	var retrierFactory func() *utils.Retrier[struct{}]
	if params.RetryBehaviour.IsEnabled {
		retrierFactory = utils.NewExponentialRetrierFactory[struct{}](
			params.RetryBehaviour.MaxRetries,
			time.Duration(params.RetryBehaviour.InitialDelayMilliseconds)*time.Millisecond,
			params.RetryBehaviour.JitterPercentage,
			time.Duration(params.RetryBehaviour.MaxDelayMilliseconds)*time.Millisecond,
		)
	} else {
		retrierFactory = utils.NewNopRetrierFactory[struct{}]()
	}
	if params.PassivationIntervalMillis == 0 {
		params.PassivationIntervalMillis = 1000
	}

	phyPartitionManagerFactory := dyndao.NewPhysicalPartitionManagerFactoryImpl(client, params.WorkerId, entityLoader, timestampCollectorFactory, retrierFactory)

	periodicTimer := time.NewTicker(time.Duration(params.PeriodicTimerMillis) * time.Millisecond)
	pollInboxTimer := time.NewTicker(time.Duration(params.PollingTimerMillis) * time.Millisecond)
	parkingTimer := time.NewTicker(time.Duration(params.PassivatingTimerMillis) * time.Millisecond)
	notifySignal := make(chan time.Time, 1)
	parkNotifySignal := make(chan time.Time, 1)

	if params.Amqp {
		notifier, err := notification.NewMQReceiver(params.WorkerId)

		if err != nil {
			log.Fatalf("failed to create AMQP notifier %s", err)
		}

		go func() {
			notifier.Start(notifySignal, parkNotifySignal)
		}()
	}

	notificationStorageFactory := notification.NewNotificationStorageFactoryImpl(params.WorkerId)

	return NewWorker(
		periodicTimer.C,
		pollInboxTimer.C,
		parkingTimer.C,
		notifySignal,
		parkNotifySignal,
		params.WorkerId,
		params.RunId,
		params.MaxActorsCount,
		params.MaximumConcurrentShardPullsCount,
		params.MaxConcurrentProcessingActors, params.MaxMessageProcessingRetries,
		params.MaximumSubsequentEmptyPhyPartitionsPullCount, params.UseBackoffStrategy, params.IdleMillisecondsToWaitBeforeParking, params.PassivationIntervalMillis,
		&taskDao, phyPartitionManagerFactory, notificationStorageFactory,
		retrierFactory,
		params.Amqp,
	)
}
