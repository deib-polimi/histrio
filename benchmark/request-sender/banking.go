package request_sender

import (
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"log"
	"main/baseline/banking/model"
	"main/baseline/banking/services"
	"main/benchmark"
	"main/lambdautils"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func SendAndMeasureBaselineBankingRequests(
	params BaselineBankingRequestsParameters,
	sender RequestSender[model.TransactionRequest, model.TransactionResponse],
	timeLogger *benchmark.RequestTimeLoggerImpl) {

	var requestSenderWg sync.WaitGroup

	timeLogger.Start()
	defer timeLogger.Stop()

	inputQueue := make(chan model.TransactionRequest, params.MaxConcurrentRequests)
	for range params.MaxConcurrentRequests {
		requestSenderWg.Add(1)
		go handleBankingRequest(inputQueue, &requestSenderWg, sender, timeLogger)
	}

	for i := range params.ActiveAccountsCount {
		for j, transactionRequest := range buildBankingRequestsForUser(i, params) {
			if j%params.MaxConcurrentRequests == 0 && params.SendingPeriodMillis != -1 {
				time.Sleep(time.Duration(params.SendingPeriodMillis) * time.Millisecond)
			}
			inputQueue <- transactionRequest
		}
	}
	close(inputQueue)
	requestSenderWg.Wait()

}

func handleBankingRequest(inputChannel chan model.TransactionRequest, wg *sync.WaitGroup,
	requestSender RequestSender[model.TransactionRequest, model.TransactionResponse],
	timeLogger *benchmark.RequestTimeLoggerImpl) {
	for transactionRequest := range inputChannel {
		err := timeLogger.LogStartRequest(transactionRequest.TransactionId)
		if err != nil {
			log.Printf("Could not log the start request %v: %v\n", transactionRequest.TransactionId, err)
		}
		_, err = requestSender.Send(transactionRequest)
		if err != nil {
			log.Printf("Failed to execute request with id %v: %v\n", transactionRequest.TransactionId, err)
		}
		err = timeLogger.LogEndRequest(transactionRequest.TransactionId)
		if err != nil {
			log.Printf("Could not log the end request %v: %v\n", transactionRequest.TransactionId, err)
		}
	}
	wg.Done()

}

func buildBankingRequestsForUser(accountIndex int, params BaselineBankingRequestsParameters) []model.TransactionRequest {
	var transactionRequests []model.TransactionRequest

	srcAccountId := "Account/" + strconv.Itoa(accountIndex)

	for range params.TransactionsPerAccount {
		dstAccountNumber := rand.Intn(params.ActiveAccountsCount)
		if dstAccountNumber == accountIndex {
			dstAccountNumber = (dstAccountNumber + 1) % params.ActiveAccountsCount
		}
		dstAccountId := "Account/" + strconv.Itoa(dstAccountNumber)
		amount := rand.Intn(500)
		transactionRequests = append(transactionRequests, model.NewTransactionRequest(
			srcAccountId,
			dstAccountId,
			amount,
		))
	}

	return transactionRequests
}

type BankingServiceSender struct {
	BankingService *services.BankingService
}

func NewBankingServiceSender(hotelService *services.BankingService) *BankingServiceSender {
	return &BankingServiceSender{BankingService: hotelService}
}

func (s *BankingServiceSender) Send(request model.TransactionRequest) (model.TransactionResponse, error) {
	return s.BankingService.ExecuteTransaction(request)
}

type LambdaBaselineBankingSender struct {
	lambdaClient *lambda.Client
}

func NewLambdaBaselineBankingSender(lambdaClient *lambda.Client) *LambdaBaselineBankingSender {
	return &LambdaBaselineBankingSender{lambdaClient: lambdaClient}
}

func (lhs *LambdaBaselineBankingSender) Send(request model.TransactionRequest) (model.TransactionResponse, error) {
	return lambdautils.InvokeBaselineBankingServiceSync(lhs.lambdaClient, request)
}

type BaselineBankingRequestsParameters struct {
	ActiveAccountsCount    int
	TransactionsPerAccount int

	SendingPeriodMillis   int
	MaxConcurrentRequests int
}
