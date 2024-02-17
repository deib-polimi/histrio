package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	bankingdb "main/baseline/banking/db"
	bankingservices "main/baseline/banking/services"
	"main/baseline/hotel-reservation/db"
	"main/baseline/hotel-reservation/services"
	"main/benchmark"
	"main/benchmark/baseline"
	request_sender "main/benchmark/request-sender"
	"main/benchmark/sut"
	"main/dynamoutils"
	"main/lambdautils"
	"main/utils"
	"main/worker/infrastructure"
	"main/worker/plugins"
	"net/http"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"time"
)

func main() {
	args := os.Args
	var client *dynamodb.Client
	isLocalDeployment := !slices.Contains(args, "aws")
	if !isLocalDeployment {
		client = dynamoutils.CreateAwsClient()
	} else {
		client = dynamoutils.CreateLocalClient()
		logErr := utils.SetLogger("LoaderLog")
		if logErr != nil {
			log.Fatalf("Could not correctly setup the logger: %v", logErr)
		}
	}

	var runSpecificParams RunSpecificParams
	b, err := os.ReadFile(path.Join(getParamsPath(), "run-specific-params.json"))
	if err != nil {
		log.Fatalf("Cannot load run-specific-params.json: %v", err)
	}

	err = json.Unmarshal(b, &runSpecificParams)

	if err != nil {
		log.Fatalf("Cannot deserialize run-specific-params.json: %v", err)
	}

	possibleCommands := []string{
		"baselineHotelLoadState", "baselineBankingLoadState", "sutHotelLoadState",
		"sutBankingLoadState", "sutBankingStartLatencyBenchmark", "sutHotelStartLatencyBenchmark", "sutHotelLoadMessages", "sutBankingLoadMessages",
		"baselineHotelSendMessages", "baselineBankingSendMessages",
		"sutRunWorkers", "timeServer", "randomlyAssignTasks",
	}
	if slices.Contains(args, "baselineHotelLoadState") {
		err = loadBaselineHotelReservationState(client)
	} else if slices.Contains(args, "baselineBankingLoadState") {
		err = loadBaselineBankingState(client)
	} else if slices.Contains(args, "sutHotelLoadState") {
		err = loadHotelReservation(client)
	} else if slices.Contains(args, "sutBankingLoadState") {
		err = loadBankingState(client)
	} else if slices.Contains(args, "sutBankingStartLatencyBenchmark") {
		err = startBankingLatencyBenchmark(client, isLocalDeployment, runSpecificParams.RunId)
	} else if slices.Contains(args, "sutHotelStartLatencyBenchmark") {
		err = startHotelLatencyBenchmark(client, isLocalDeployment, runSpecificParams.RunId)
	} else if slices.Contains(args, "sutHotelLoadMessages") {
		err = loadHotelReservationInboxesAndTasks(client)
	} else if slices.Contains(args, "sutBankingLoadMessages") {
		err = loadBankingInboxesAndTasks(client)
	} else if slices.Contains(args, "baselineHotelSendMessages") {
		err = sendBaselineHotelReservationRequests(client, isLocalDeployment, runSpecificParams.RunId, runSpecificParams.ConcurrentLogsCount)
	} else if slices.Contains(args, "baselineBankingSendMessages") {
		err = sendBaselineBankingRequests(client, isLocalDeployment, runSpecificParams.RunId, runSpecificParams.ConcurrentLogsCount)
	} else if slices.Contains(args, "sutRunWorkers") {
		err = startWorkers(isLocalDeployment, runSpecificParams.RunId)
	} else if slices.Contains(args, "timeServer") {
		err = startTimeLogServer(isLocalDeployment, runSpecificParams.RunId, runSpecificParams.ConcurrentLogsCount)
	} else if slices.Contains(args, "randomlyAssignTasks") {
		err = randomlyAssignTasks(client)
	} else {
		log.Fatalf("No command inserted. Please use one of the following: %v", possibleCommands)
	}
	if err != nil {
		log.Fatal(err)
	}

}

func loadHotelReservation(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-hotel-reservation-params.json"))
	if err != nil {
		return err
	}
	parameters := &sut.HotelReservationParameters{}
	err = json.Unmarshal(b, parameters)
	if err != nil {
		return err
	}

	return sut.HotelReservationLoadState(parameters, client)
}

func loadBankingState(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-banking-params.json"))
	if err != nil {
		return err
	}
	parameters := &sut.BankingParameters{}
	err = json.Unmarshal(b, parameters)
	if err != nil {
		return err
	}

	return sut.BankingLoadState(parameters, client)
}

func loadBaselineHotelReservationState(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "baseline-hotel-reservation-state-params.json"))
	if err != nil {
		return err
	}
	var params baseline.BaselineHotelReservationParameters
	err = json.Unmarshal(b, &params)

	if err != nil {
		return err
	}

	return baseline.LoadBaselineHotelReservationState(params, client)
}

func loadBaselineBankingState(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "baseline-banking-state-params.json"))
	if err != nil {
		return err
	}
	var params baseline.BaselineBankingParameters
	err = json.Unmarshal(b, &params)

	if err != nil {
		return err
	}
	return baseline.LoadBaselineBankingState(params, client)
}

func startHotelLatencyBenchmark(client *dynamodb.Client, isLocalDeployment bool, runId string) error {
	reservationParamsBytes, err := os.ReadFile(path.Join(getParamsPath(), "sut-hotel-reservation-params.json"))
	if err != nil {
		return err
	}

	slowLoadingParamsBytes, err := os.ReadFile(path.Join(getParamsPath(), "sut-hotel-reservation-slow-loading-params.json"))
	if err != nil {
		return err
	}

	parameters := &sut.HotelReservationParameters{}
	slowLoadingParams := &sut.SlowLoadingParams{}

	err = json.Unmarshal(reservationParamsBytes, parameters)
	if err != nil {
		return err
	}

	err = json.Unmarshal(slowLoadingParamsBytes, slowLoadingParams)
	if err != nil {
		return err
	}

	newMessage, newTasks := sut.HotelReservationBuildInboxesAndTasks(parameters)
	err = dynamoutils.AddActorTaskBatch(client, newTasks)
	if err != nil {
		return err
	}

	err = randomlyAssignTasks(client)
	if err != nil {
		return err
	}

	err = startWorkers(isLocalDeployment, runId)
	if err != nil {
		return err
	}

	return sut.SlowlyLoadInboxes(
		newMessage,
		client,
		time.Duration(slowLoadingParams.SendingPeriodMillis)*time.Millisecond,
		slowLoadingParams.MaxRequestsPerPeriod,
		time.Duration(slowLoadingParams.InitialDelayMillis)*time.Millisecond,
	)

}

func startBankingLatencyBenchmark(client *dynamodb.Client, isLocalDeployment bool, runId string) error {
	bankingParamsBytes, err := os.ReadFile(path.Join(getParamsPath(), "sut-banking-params.json"))
	if err != nil {
		return err
	}

	slowLoadingParamsBytes, err := os.ReadFile(path.Join(getParamsPath(), "slow-loading-params.json"))
	if err != nil {
		return err
	}

	parameters := &sut.BankingParameters{}
	slowLoadingParams := &sut.SlowLoadingParams{}

	err = json.Unmarshal(bankingParamsBytes, parameters)
	if err != nil {
		return err
	}

	err = json.Unmarshal(slowLoadingParamsBytes, slowLoadingParams)
	if err != nil {
		return err
	}

	newMessage, newTasks := sut.BankingBuildInboxesAndTasks(parameters)
	err = dynamoutils.AddActorTaskBatch(client, newTasks)
	if err != nil {
		return err
	}

	err = randomlyAssignTasks(client)
	if err != nil {
		return err
	}

	err = startWorkers(isLocalDeployment, runId)
	if err != nil {
		return err
	}

	return sut.SlowlyLoadInboxes(
		newMessage,
		client,
		time.Duration(slowLoadingParams.SendingPeriodMillis)*time.Millisecond,
		slowLoadingParams.MaxRequestsPerPeriod,
		time.Duration(slowLoadingParams.InitialDelayMillis)*time.Millisecond,
	)
}

func loadHotelReservationInboxesAndTasks(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-hotel-reservation-params.json"))
	if err != nil {
		return err
	}

	parameters := &sut.HotelReservationParameters{}
	err = json.Unmarshal(b, parameters)
	if err != nil {
		return err
	}

	return sut.HotelReservationLoadInboxesAndTasks(parameters, client)
}

func loadBankingInboxesAndTasks(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-banking-params.json"))
	if err != nil {
		return err
	}

	parameters := &sut.BankingParameters{}
	err = json.Unmarshal(b, parameters)
	if err != nil {
		return err
	}

	return sut.BankingLoadInboxesAndTasks(parameters, client)
}

func sendBaselineHotelReservationRequests(client *dynamodb.Client, isLocalDeployment bool, runId string, concurrentLogsCount int) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "baseline-hotel-reservation-requests-params.json"))
	if err != nil {
		return err
	}
	var requestsParameters request_sender.BaselineBookingRequestsParameters

	basePath := getTimeLoggerPath()
	if isLocalDeployment {
		basePath = path.Join(filepath.Dir(utils.Root), "log")
	}

	timeLogger := benchmark.NewRequestTimeLoggerImpl(basePath, runId, concurrentLogsCount)

	err = json.Unmarshal(b, &requestsParameters)
	if isLocalDeployment {
		hotelServiceDao := db.NewHotelDynDao(client, "asdfasdf")
		hotelService := services.NewReservationService(hotelServiceDao)
		request_sender.SendAndMeasureBaselineBookingRequests(requestsParameters, request_sender.NewServiceSender(hotelService), timeLogger)
		return nil
	} else {
		lambdaClient := lambdautils.CreateNewClient()
		request_sender.SendAndMeasureBaselineBookingRequests(requestsParameters, request_sender.NewLambdaBaselineHotelSender(lambdaClient), timeLogger)
		return nil
	}

}

func sendBaselineBankingRequests(client *dynamodb.Client, isLocalDeployment bool, runId string, concurrentLogsCount int) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "baseline-banking-requests-params.json"))
	if err != nil {
		return err
	}
	var requestsParameters request_sender.BaselineBankingRequestsParameters

	basePath := getTimeLoggerPath()
	if isLocalDeployment {
		basePath = path.Join(filepath.Dir(utils.Root), "log")
	}

	timeLogger := benchmark.NewRequestTimeLoggerImpl(basePath, runId, concurrentLogsCount)

	err = json.Unmarshal(b, &requestsParameters)
	if isLocalDeployment {
		bankingServiceDao := bankingdb.NewAccountDynDao(client, "asdfasdf")
		bankingService := bankingservices.NewBankingService(bankingServiceDao)
		request_sender.SendAndMeasureBaselineBankingRequests(requestsParameters, request_sender.NewBankingServiceSender(bankingService), timeLogger)
		return nil
	} else {
		lambdaClient := lambdautils.CreateNewClient()
		request_sender.SendAndMeasureBaselineBankingRequests(requestsParameters, request_sender.NewLambdaBaselineBankingSender(lambdaClient), timeLogger)
		return nil
	}
}

func startWorkers(isLocalDeployment bool, runId string) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-run-workers.json"))
	if err != nil {
		return err
	}

	var parallelWorkersInput ParallelWorkersInput

	err = json.Unmarshal(b, &parallelWorkersInput)
	if err != nil {
		return err
	}

	parallelWorkersInput.RunId = runId

	parallelWorkersCount := parallelWorkersInput.ParallelWorkersCount

	if parallelWorkersCount <= 0 {
		return errors.New("cannot have a non positive number of parallel workers")
	}

	var workerParamsList []infrastructure.WorkerParameters

	for i := range parallelWorkersCount {
		workerParameters := parallelWorkersInput.WorkerParams
		workerParameters.WorkerId = "Worker-" + strconv.Itoa(i)
		workerParameters.RunId = parallelWorkersInput.RunId
		if !infrastructure.IsWorkerParametersValid(&workerParameters) {
			return errors.New("worker parameters are not valid")
		}
		workerParamsList = append(workerParamsList, workerParameters)
	}

	if isLocalDeployment {
		dynClient := dynamoutils.CreateLocalClient()
		var wg sync.WaitGroup
		for i := range parallelWorkersCount {
			wg.Add(1)
			worker := infrastructure.BuildNewWorker(&workerParamsList[i], dynClient, plugins.NewTimestampCollectorFactoryLocalImpl())
			go func() {
				worker.Run()
				wg.Done()
			}()
		}

		wg.Wait()

	} else {
		lambdaClient := lambdautils.CreateNewClient()
		for i := range parallelWorkersCount {
			err = lambdautils.InvokeWorkerAsync(lambdaClient, workerParamsList[i])
			if err != nil {
				return err
			}
		}
	}

	return nil

}

func startTimeLogServer(isLocalDeployment bool, runId string, concurrentLogsCount int) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "time-server.json"))
	if err != nil {
		return err
	}

	var params TimeLoggerServerParams

	err = json.Unmarshal(b, &params)

	if err != nil {
		return err
	}

	basePath := getTimeLoggerPath()
	if isLocalDeployment {
		basePath = path.Join(filepath.Dir(utils.Root), "log")
		log.Printf("BASE PATH: %v\n", basePath)
	}

	timeLogger := benchmark.NewRequestTimeLoggerImpl(basePath, runId, concurrentLogsCount)
	timeLogger.Start()
	defer timeLogger.Stop()

	server := &http.Server{Addr: ":" + params.Port}

	startPattern := regexp.MustCompile(`/start/(.*)$`)
	endPattern := regexp.MustCompile(`/end/(.*)$`)

	http.HandleFunc("/start/", func(writer http.ResponseWriter, request *http.Request) {
		requestPath := request.URL.Path
		match := startPattern.FindAllStringSubmatch(requestPath, -1)
		if len(match) == 0 || len(match[0]) < 2 {
			log.Printf("Malformed request: %v\n", requestPath)
			return
		}
		identifier := match[0][1]
		err := timeLogger.LogStartRequest(identifier)
		if err != nil {
			log.Printf("Could not log the start of request %v: %v\n", identifier, err)
		}
	})

	http.HandleFunc("/end/", func(writer http.ResponseWriter, request *http.Request) {
		requestPath := request.URL.Path
		match := endPattern.FindAllStringSubmatch(requestPath, -1)
		if len(match) == 0 || len(match[0]) < 2 {
			log.Printf("Malformed request: %v\n", requestPath)
			return
		}
		identifier := match[0][1]
		err := timeLogger.LogEndRequest(identifier)
		if err != nil {
			log.Printf("Could not log the end of request %v: %v\n", identifier, err)
		}
	})

	go func() {
		err := server.ListenAndServe()

		if errors.Is(err, http.ErrServerClosed) {
			log.Println("The server has been shut down")
		} else if err != nil {
			log.Printf("server error: %v\n", err)
		}
	}()

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt)

	<-stop

	return server.Shutdown(context.TODO())

}

func randomlyAssignTasks(client *dynamodb.Client) error {
	b, err := os.ReadFile(path.Join(getParamsPath(), "sut-run-workers.json"))
	if err != nil {
		return err
	}

	var parallelWorkersInput ParallelWorkersInput

	err = json.Unmarshal(b, &parallelWorkersInput)
	if err != nil {
		return err
	}

	return dynamoutils.EquallyAssignTasksToWorkers(client, parallelWorkersInput.ParallelWorkersCount)
}

func getTimeLoggerPath() string {
	return path.Join(getExecPath(), "time-logs")
}

func getParamsPath() string {
	return path.Join(getExecPath(), "params")
}

func getExecPath() string {
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := filepath.Dir(ex)
	return exPath
}

type ParallelWorkersInput struct {
	WorkerParams         infrastructure.WorkerParameters
	ParallelWorkersCount int
	RunId                string
}

type TimeLoggerServerParams struct {
	Port string
}

type RunSpecificParams struct {
	RunId               string
	ConcurrentLogsCount int
}
