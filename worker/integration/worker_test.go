package integration

import (
	"fmt"
	"log"
	bankingdb "main/baseline/banking/db"
	bankingmodel "main/baseline/banking/model"
	bankingservices "main/baseline/banking/services"
	"main/baseline/hotel-reservation/db"
	"main/baseline/hotel-reservation/model"
	"main/baseline/hotel-reservation/services"
	"main/benchmark/baseline"
	"main/benchmark/sut"
	"main/dynamoutils"
	worker_simulation "main/experiments/worker-simulation"
	"main/utils"
	"main/worker/domain"
	"main/worker/infrastructure"
	"main/worker/plugins"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"main/worker/dyndao"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
)

func WorkerTestSetup(client *dynamodb.Client) {
	existingTableNames, err := dynamoutils.GetExistingTableNames(client)

	if err != nil {
		log.Fatal(err)
	}

	if !slices.Contains(existingTableNames, "ActorState") {
		dynamoutils.CreateActorStateTable(client)
	}

	if !slices.Contains(existingTableNames, "ActorInbox") {
		dynamoutils.CreateActorInboxTable(client)
	}

	if !slices.Contains(existingTableNames, "ActorTask") {
		dynamoutils.CreateActorTaskTable(client)
	}

	if !slices.Contains(existingTableNames, "WeekAvailability") {
		dynamoutils.CreateEntityTable(client, "WeekAvailability", &domain.WeekAvailability{})
	}

	if !slices.Contains(existingTableNames, "Account") {
		dynamoutils.CreateEntityTable(client, "Account", &domain.Account{})
	}

	if !slices.Contains(existingTableNames, "Partitions") {
		dynamoutils.CreatePartitionTable(client)
	}

	if !slices.Contains(existingTableNames, "Outbox") {
		dynamoutils.CreateOutboxTable(client)
	}

	if !slices.Contains(existingTableNames, "BaselineTable") {
		dynamoutils.CreateBaselineTable(client)
	}

	if !slices.Contains(existingTableNames, "Journey") {
		dynamoutils.CreateEntityTable(client, "Journey", &domain.Journey{})
	}

}

func WorkerTestCleanup(client *dynamodb.Client) {
	dynamoutils.DeleteTable(client, "ActorState")
	dynamoutils.DeleteTable(client, "ActorInbox")
	dynamoutils.DeleteTable(client, "ActorTask")
	dynamoutils.DeleteTable(client, "WeekAvailability")
	dynamoutils.DeleteTable(client, "Account")
	dynamoutils.DeleteTable(client, "Partitions")
	dynamoutils.DeleteTable(client, "Outbox")
	dynamoutils.DeleteTable(client, "BaselineTable")
	dynamoutils.DeleteTable(client, "Journey")
}

func buildExternalHandler(client *dynamodb.Client) domain.ExternalCommandHandler {
	actorSpawningDao := dyndao.NewDynActorSpawningDao(client)
	actorSpawner := domain.NewActorSpawner(actorSpawningDao, 1*time.Second, 20, 5)
	messageStorer := dyndao.NewDynMessageStorerDao(client)
	return domain.NewExternalHandler(actorSpawner, actorSpawningDao, messageStorer)
}

func TestWorkerSetup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	WorkerTestSetup(client)
}

func TestPopulateActorTasksOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	actorsCount := 100
	physicalPartitions := 10
	messagesPerActor := 10

	for i := range actorsCount {
		actorId := domain.ActorId{
			InstanceId: strconv.Itoa(i),
			PhyPartitionId: domain.PhysicalPartitionId{
				PartitionName:         "MyPartition",
				PhysicalPartitionName: strconv.Itoa(i % physicalPartitions),
			},
		}
		otherActorId := domain.ActorId{
			InstanceId: strconv.Itoa(i) + "-test",
			PhyPartitionId: domain.PhysicalPartitionId{
				PartitionName:         "MyPartition",
				PhysicalPartitionName: actorId.PhyPartitionId.PhysicalPartitionName + "-test",
			},
		}
		dynamoutils.AddActorState(client, actorId, &domain.MyActor{Id: actorId})
		dynamoutils.AddActorState(client, otherActorId, &domain.MyActor{Id: otherActorId})
		dynamoutils.AddActorTask(client, actorId.PhyPartitionId, false, "NULL")
		separator := ""
		for j := range messagesPerActor {
			message := domain.ActorMessage{
				Id: domain.MessageIdentifier{
					ActorId:         actorId,
					UniqueTimestamp: "",
				},
				SenderId: domain.ActorId{
					InstanceId:     "-",
					PhyPartitionId: domain.PhysicalPartitionId{PartitionName: "", PhysicalPartitionName: ""},
				},
				Content: domain.SimpleMessage{Content: separator + strconv.Itoa(j)},
			}
			dynamoutils.AddMessage(client, message, actorId)
			separator = "|"
		}
	}
}

func TestRunWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	worker := createDefaultWorker("1", 5, 5, 500, 500)
	worker.Run()
}

func TestWorkerCleanAndSetupAgain(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	TestWorkerCleanup(t)
	TestWorkerSetup(t)
	TestPopulateActorTasksOnly(t)

}

func TestPullTasksOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	client := dynamoutils.CreateLocalClient()
	workerDao := dyndao.DynTaskDao{Client: client}
	tasks, err := workerDao.PullNewActorTasks("0", 100)

	if err != nil {
		t.Fatal(err)
	}

	t.Logf("Pulled %v tasks", len(tasks))
}

func TestWorkerCleanup(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	WorkerTestCleanup(client)
}

func TestWorker(t *testing.T) {
	TestWorkerCleanAndSetupAgain(t)
	fmt.Println("START WORKING")
	startTime := time.Now()
	TestRunWorker(t)
	fmt.Printf("Processing time: %vs\n", time.Since(startTime).Seconds())

}

func TestMultipleWorkers(t *testing.T) {
	TestWorkerCleanAndSetupAgain(t)
	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	worker1 := createDefaultWorker("1", 10, 10, 500, 500)
	worker2 := createDefaultWorker("2", 10, 10, 500, 500)

	fmt.Println("START WORKING")

	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		worker1.Run()
		wg.Done()
	}()
	go func() {
		worker2.Run()
		wg.Done()
	}()

	wg.Wait()
}

func TestWorkerSimulation(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestSimulatedWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	worker_simulation.WorkerMain(100, 1000)
}

func createDefaultWorker(
	workerId string, maxActorsCount int, maxConcurrentProcessingActors int,
	pollingTimeMillis int64, periodicTimerMillis int64) *infrastructure.Worker {
	client := dynamoutils.CreateLocalClient()
	workerParams := infrastructure.NewWorkerParameters(
		workerId,
		"Run0",
		"",
		maxActorsCount,
		10,
		maxConcurrentProcessingActors,
		2,
		6,
		false,
		5000,
		periodicTimerMillis,
		pollingTimeMillis,
		periodicTimerMillis,
		infrastructure.RetryBehaviourParams{},
	)

	if !infrastructure.IsWorkerParametersValid(workerParams) {
		log.Fatal("Worker params are not valid")
	}

	return infrastructure.BuildNewWorker(workerParams, client, plugins.NewTimestampCollectorFactoryLocalImpl())
}

//----------------------------------------------------------------------------------------------------------------------
// TEST WITH MY_ACTOR THAT SPAWNS ANOTHER ACTOR
//----------------------------------------------------------------------------------------------------------------------

func TestPopulateMyActorSpawningOtherActorScenario(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()

	actorId := domain.ActorId{
		InstanceId: strconv.Itoa(0),
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "MyPartition",
			PhysicalPartitionName: "1",
		},
	}

	dynamoutils.AddActorState(client, actorId, &domain.MyActor{Id: actorId})
	dynamoutils.AddActorTask(client, actorId.PhyPartitionId, false, "NULL")

	message := domain.ActorMessage{
		Id: domain.MessageIdentifier{
			ActorId:         actorId,
			UniqueTimestamp: "",
		},
		SenderId: domain.ActorId{
			InstanceId:     "-",
			PhyPartitionId: domain.PhysicalPartitionId{PartitionName: "", PhysicalPartitionName: ""},
		},
		Content: domain.SimpleMessage{Content: "SPAWN"},
	}
	dynamoutils.AddMessage(client, message, actorId)

	externalHandler := buildExternalHandler(client)
	externalHandler.CreatePartition("MyPartition")
}

func TestMyActorSpawningOtherActorWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	TestPopulateMyActorSpawningOtherActorScenario(t)
	worker := createDefaultWorker("1", 500, 50, 500, 500)
	worker.Run()
}

//----------------------------------------------------------------------------------------------------------------------
// TEST WITH HOTELS
//----------------------------------------------------------------------------------------------------------------------

func TestRunHotelWorker(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	TestPopulateHotelScenario(t)
	worker := createDefaultWorker("1", 300, 5, 100, 500)
	worker.Run()
}

func TestRunHotelMultipleWorkers(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	TestPopulateHotelScenario(t)
	worker1 := createDefaultWorker("1", 500, 50, 50, 500)
	worker2 := createDefaultWorker("2", 500, 50, 50, 500)
	wg := sync.WaitGroup{}
	wg.Add(2)
	go func() {
		worker1.Run()
		wg.Done()
	}()
	go func() {
		worker2.Run()
		wg.Done()
	}()

	wg.Wait()

}

func TestPopulateHotelScenario(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()

	parameters := &sut.HotelReservationParameters{
		HotelsPartitionsCount:         1,
		HotelsShardsPerPartitionCount: 10,
		HotelsPerShardCount:           10,
		WeeksCount:                    4,
		RoomsPerTypeCount:             10,
		UserShardsPerPartitionCount:   2,
		UsersPerShardCount:            5,

		ActiveHotelPartitionsCount:         1,
		ActiveHotelShardsPerPartitionCount: 10,
		ActiveHotelsPerShardCount:          10,
		ActiveWeeksCount:                   4,
		ActiveUserPartitionsCount:          20,
		ActiveUserShardsCount:              2,
		ActiveUsersPerShardCount:           5,
		RequestsPerUser:                    10,
	}

	err := sut.PopulateHotelReservationScenario(parameters, client)

	if err != nil {
		t.Fatal(err)
	}

}

//----------------------------------------------------------------------------------------------------------------------
// TEST WITH BANKING
//----------------------------------------------------------------------------------------------------------------------

func TestRunBankingWorker(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	client := dynamoutils.CreateLocalClient()

	bankingParameters := sut.BankingParameters{
		BankPartitionsCount:         5,
		BankShardsPerPartitionCount: 4,
		BanksPerShardCount:          5,
		AccountsPerBankCount:        100,

		ActiveBankPartitionsCount:         5,
		ActiveBankShardsPerPartitionCount: 2,
		ActiveBanksPerShardCount:          4,
		ActiveAccountsPerBankCount:        15,
		TransactionsPerAccountCount:       2,
	}

	err := sut.BankingLoadState(&bankingParameters, client)
	if err != nil {
		t.Fatal(err)
	}

	err = sut.BankingLoadInboxesAndTasks(&bankingParameters, client)
	if err != nil {
		t.Fatal(err)
	}
	fmt.Println("START WORKER")
	worker := createDefaultWorker("1", 50, 50, 100, 500)
	worker.Run()
}

//----------------------------------------------------------------------------------------------------------------------
// TEST WITH BASELINE HOTELS
//----------------------------------------------------------------------------------------------------------------------

func TestBaselineHotels(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	client := dynamoutils.CreateLocalClient()

	params := baseline.BaselineHotelReservationParameters{
		HotelsCount:       5,
		WeeksCount:        4,
		RoomsPerTypeCount: 5,
		UsersCount:        9,
	}

	err := baseline.LoadBaselineHotelReservationState(params, client)
	if err != nil {
		t.Fatal(err)
	}
	uniqueId := "asfd"
	hotelServiceDao := db.NewHotelDynDao(client, uniqueId)
	hotelService := services.NewReservationService(hotelServiceDao)
	bookingResponse, err := hotelService.ReserveRoom(model.BookingRequest{
		RequestId: "asdf",
		UserId:    "User/0",
		HotelId:   "Hotel/0",
		RoomType:  model.STANDARD,
		BookingPeriod: model.BookingPeriod{
			Week:      "0",
			DayOfWeek: 0,
		},
	})

	log.Printf("%v", bookingResponse.Success)

	if err != nil {
		t.Fatal(err)
	}
}

//----------------------------------------------------------------------------------------------------------------------
// TEST WITH BASELINE BANKING
//----------------------------------------------------------------------------------------------------------------------

func TestBaselineBanking(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	client := dynamoutils.CreateLocalClient()

	params := baseline.BaselineBankingParameters{
		AccountsCount: 10,
	}

	err := baseline.LoadBaselineBankingState(params, client)
	if err != nil {
		t.Fatal(err)
	}

	var myServices []*bankingservices.BankingService
	servicesCount := 100

	for i := range servicesCount {
		myServices = append(myServices, bankingservices.NewBankingService(bankingdb.NewAccountDynDao(client, strconv.Itoa(i))))
	}

	var wg sync.WaitGroup
	for index, myService := range myServices {
		wg.Add(1)
		go func(index int, localService *bankingservices.BankingService) {

			sourceIndex := index % params.AccountsCount    //rand.IntN(params.AccountsCount)
			dstIndex := (index + 1) % params.AccountsCount //rand.IntN(params.AccountsCount)
			if dstIndex == sourceIndex {
				dstIndex = (sourceIndex + 1) % params.AccountsCount
			}
			_, localErr := localService.ExecuteTransaction(bankingmodel.TransactionRequest{
				TransactionId:   strconv.Itoa(index),
				SourceIban:      "Account/" + strconv.Itoa(sourceIndex),
				DestinationIban: "Account/" + strconv.Itoa(dstIndex),
				Amount:          10,
			})
			if localErr != nil {
				log.Printf("Service failed with error: %v\n", localService)
			}
			wg.Done()
		}(index, myService)
	}

	wg.Wait()
}

//----------------------------------------------------------------------------------------------------------------------
// REQUEST SENDER TEST
//----------------------------------------------------------------------------------------------------------------------

func TestRequestSender(t *testing.T) {
	/*
		TestWorkerCleanup(t)
		TestWorkerSetup(t)

		if testing.Short() {
			t.Skip("skipping test in short mode.")
		}

		logErr := utils.SetLogger("TestRunWorker")
		if logErr != nil {
			t.Fatal(logErr)
		}

		client := dynamoutils.CreateLocalClient()

		params := baseline.BaselineHotelReservationParameters{
			HotelsCount:       5,
			WeeksCount:        4,
			RoomsPerTypeCount: 5,
			UsersCount:        9,
		}
		err := baseline.LoadBaselineHotelReservationState(params, client)
		if err != nil {
			t.Fatal(err)
		}

		requestsParameters := request_sender.BaselineBookingRequestsParameters{
			ActiveHotelsCount:        5,
			ActiveWeeksPerHotelCount: 4,
			ActiveUsersCount:         9,
			RequestsPerUser:          2,
			MaxConcurrentRequests:    100,
		}

		hotelServiceDao := db.NewHotelDynDao(client, "asdfasdf")
		hotelService := services.NewReservationService(hotelServiceDao)

		request_sender.SendAndMeasureBaselineBookingRequests(requestsParameters, request_sender.NewServiceSender(hotelService))
	*/
}

// ----------------------------------------------------------------------------------------------------------------------
// SLOW SENDER TEST
// ----------------------------------------------------------------------------------------------------------------------
func TestSlowSender(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()

	parameters := &sut.HotelReservationParameters{
		HotelsPartitionsCount:         1,
		HotelsShardsPerPartitionCount: 10,
		HotelsPerShardCount:           10,
		WeeksCount:                    4,
		RoomsPerTypeCount:             10,
		UserShardsPerPartitionCount:   2,
		UsersPerShardCount:            5,

		ActiveHotelPartitionsCount:         1,
		ActiveHotelShardsPerPartitionCount: 10,
		ActiveHotelsPerShardCount:          10,
		ActiveWeeksCount:                   4,
		ActiveUserPartitionsCount:          20,
		ActiveUserShardsCount:              2,
		ActiveUsersPerShardCount:           5,
		RequestsPerUser:                    10,
	}

	newMessages, _ := sut.HotelReservationBuildInboxesAndTasks(parameters)

	err := sut.SlowlyLoadInboxes(newMessages, client, time.Duration(2000)*time.Millisecond, 5, time.Duration(2000)*time.Millisecond)

	if err != nil {
		t.Fatal(err)
	}
}

// ---------------------------------------------------------------------------------------------------------------------
// TRAVEL AGENCY TEST
// ---------------------------------------------------------------------------------------------------------------------

func TestTravelAgencyDiscount(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	actorId := domain.ActorId{
		InstanceId: strconv.Itoa(0),
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "City1",
			PhysicalPartitionName: "1",
		},
	}

	dynamoutils.AddActorState(client, actorId, &domain.TravelAgency{Id: actorId})
	dynamoutils.AddActorTask(client, actorId.PhyPartitionId, false, "NULL")

	message := domain.ActorMessage{
		Id: domain.MessageIdentifier{
			ActorId:         actorId,
			UniqueTimestamp: "",
		},
		SenderId: domain.ActorId{
			InstanceId:     "-",
			PhyPartitionId: domain.PhysicalPartitionId{PartitionName: "", PhysicalPartitionName: ""},
		},
		Content: domain.DiscountRequest{
			Destination: "Destination2",
			Discount:    0.10,
		},
	}
	dynamoutils.AddMessage(client, message, actorId)

	var newEntities []utils.Pair[domain.CollectionId, domain.QueryableItem]
	destinations := []string{"Destination1", "Destination2", "Destination3", "Destination4"}
	for i := range 50 {
		travel := domain.Journey{
			Id:          "Journey#" + strconv.Itoa(i),
			Destination: destinations[i%len(destinations)],
			Cost:        1000,
		}
		newEntities = append(newEntities, utils.Pair[domain.CollectionId, domain.QueryableItem]{
			First:  domain.CollectionId{Id: actorId.String() + "/Catalog", TypeName: "Journey"},
			Second: &travel,
		})
	}

	dynamoutils.AddEntityBatch(client, newEntities)

	fmt.Println("START WORKER")
	worker := createDefaultWorker("1", 50, 50, 100, 500)
	worker.Run()
}

func TestTravelAgencyBooking(t *testing.T) {
	TestWorkerCleanup(t)
	TestWorkerSetup(t)

	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	logErr := utils.SetLogger("TestRunWorker")
	if logErr != nil {
		t.Fatal(logErr)
	}

	actorId := domain.ActorId{
		InstanceId: strconv.Itoa(0),
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "City1",
			PhysicalPartitionName: "1",
		},
	}

	fakeUserId := domain.ActorId{
		InstanceId: "User123423",
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "City1",
			PhysicalPartitionName: "1",
		},
	}

	dynamoutils.AddActorState(client, actorId, &domain.TravelAgency{Id: actorId})
	dynamoutils.AddActorState(client, fakeUserId, &domain.SinkActor{Id: fakeUserId})
	dynamoutils.AddActorTask(client, actorId.PhyPartitionId, false, "NULL")

	message := domain.ActorMessage{
		Id: domain.MessageIdentifier{
			ActorId:         actorId,
			UniqueTimestamp: "",
		},
		SenderId: fakeUserId,
		Content: domain.TravelBookingRequest{
			UserId:   fakeUserId,
			TravelId: "Journey#0",
		},
	}
	dynamoutils.AddMessage(client, message, actorId)

	var newEntities []utils.Pair[domain.CollectionId, domain.QueryableItem]
	destinations := []string{"Destination1", "Destination2", "Destination3", "Destination4"}
	for i := range 50 {
		travel := domain.Journey{
			Id:                "Journey#" + strconv.Itoa(i),
			Destination:       destinations[i%len(destinations)],
			Cost:              1000,
			AvailableBookings: 100,
		}
		newEntities = append(newEntities, utils.Pair[domain.CollectionId, domain.QueryableItem]{
			First:  domain.CollectionId{Id: actorId.String() + "/Catalog", TypeName: "Journey"},
			Second: &travel,
		})
	}

	dynamoutils.AddEntityBatch(client, newEntities)
	externalHandler := buildExternalHandler(client)
	externalHandler.CreatePartition("City1")

	fmt.Println("START WORKER")
	worker := createDefaultWorker("1", 50, 50, 100, 500)
	worker.Run()
}
