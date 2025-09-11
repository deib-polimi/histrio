package request_sender

import (
	"log"
	"main/baseline/banking/model"
	"main/baseline/banking/services"
	"main/lambdautils"
	"main/worker/plugins"
	"math/rand"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

func SendAndMeasureBaselineBankingRequests(
	params BaselineBankingRequestsParameters,
	sender RequestSender[model.TransactionRequest, model.TransactionResponse],
	runId string) {

	requestQueue := make(chan model.TransactionRequest, params.MaxConcurrentRequests)
	var wg sync.WaitGroup
	var httpClient = &http.Client{}
	var timeServerFactory = plugins.NewTimestampCollectorFactoryImpl(httpClient, "http://127.0.0.1:8080")

	for range params.MaxConcurrentRequests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			bankingRequestRoutine(timeServerFactory, sender, requestQueue, runId)
		}()
	}

	log.Printf("Generating messages...")
	newMessages := make([]model.TransactionRequest, 0)
	for i := range params.ActiveAccountsCount {
		newMessages = append(newMessages, buildBankingRequestsForUser(i, params)...)
	}
	log.Printf("Starting in 1s...")

	time.Sleep(time.Duration(1000) * time.Millisecond)
	start := time.Now()
	var ticker = time.NewTicker(time.Duration(params.SendingPeriodMillis) * time.Millisecond)

	i := 0
	for {
		endExcludedIndex := min(i+params.MaxConcurrentRequests, len(newMessages))

		log.Printf("%d..%d\t(%d) - %s", i, endExcludedIndex, len(newMessages), time.Since(start))

		messageBatch := newMessages[i:endExcludedIndex]

		for _, message := range messageBatch {
			requestQueue <- message
		}

		i = endExcludedIndex

		if i >= len(newMessages) {
			break
		}

		<-ticker.C
	}

	close(requestQueue)
	wg.Wait()

}

func bankingRequestRoutine(timeServerFactory *plugins.TimestampCollectorFactoryImpl, requestSender RequestSender[model.TransactionRequest, model.TransactionResponse], requestQueue <-chan model.TransactionRequest, runId string) {
	var timeServer = timeServerFactory.BuildTimestampCollector()
	for request := range requestQueue {
		// log.Printf("Request type: %T", request.First.Content)
		err := timeServer.StartMeasurement(runId + "/" + request.TransactionId)

		if err != nil {
			log.Printf("Could not log the start request %v: %v\n", request.TransactionId, err)
		}
		makeBankingRequest(request, requestSender)
		err = timeServer.EndMeasurement(runId + "/" + request.TransactionId)

		if err != nil {
			log.Printf("Could not log the end request %v: %v\n", request.TransactionId, err)
		}
	}
}
func makeBankingRequest(transactionRequest model.TransactionRequest,
	requestSender RequestSender[model.TransactionRequest, model.TransactionResponse]) {

	_, err := requestSender.Send(transactionRequest)
	if err != nil {
		log.Printf("Failed to execute request with id %v: %v\n", transactionRequest.TransactionId, err)
	}
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
