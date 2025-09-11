package request_sender

import (
	"log"
	"main/baseline/hotel-reservation/model"
	"main/baseline/hotel-reservation/services"
	"main/lambdautils"
	"main/worker/plugins"
	"math/rand/v2"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/lambda"
)

func SendAndMeasureBaselineBookingRequests(
	params BaselineBookingRequestsParameters,
	sender RequestSender[model.BookingRequest, model.BookingResponse],
	runId string) {

	requestQueue := make(chan model.BookingRequest, params.MaxConcurrentRequests)
	var wg sync.WaitGroup
	var httpClient = &http.Client{}
	var timeServerFactory = plugins.NewTimestampCollectorFactoryImpl(httpClient, "http://127.0.0.1:8080")

	for range params.MaxConcurrentRequests {
		wg.Add(1)
		go func() {
			defer wg.Done()
			requestRoutine(timeServerFactory, sender, requestQueue, runId)
		}()
	}

	hotelSeed := 0
	weekSeed := 0
	log.Printf("Generating messages...")
	newMessages := make([]model.BookingRequest, 0)
	for i := range params.ActiveUsersCount {
		newMessages = append(newMessages, buildBookingRequestsForUser(i, params, &hotelSeed, &weekSeed)...)
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

func requestRoutine(timeServerFactory *plugins.TimestampCollectorFactoryImpl, requestSender RequestSender[model.BookingRequest, model.BookingResponse], requestQueue <-chan model.BookingRequest, runId string) {
	var timeServer = timeServerFactory.BuildTimestampCollector()
	for request := range requestQueue {
		// log.Printf("Request type: %T", request.First.Content)
		err := timeServer.StartMeasurement(runId + "/" + request.RequestId)

		if err != nil {
			log.Printf("Could not log the start request %v: %v\n", request.RequestId, err)
		}
		makeBookingRequest(request, requestSender)
		err = timeServer.EndMeasurement(runId + "/" + request.RequestId)

		if err != nil {
			log.Printf("Could not log the end request %v: %v\n", request.RequestId, err)
		}
	}
}

func makeBookingRequest(bookingRequest model.BookingRequest, requestSender RequestSender[model.BookingRequest, model.BookingResponse]) {
	_, err := requestSender.Send(bookingRequest)
	if err != nil {
		log.Printf("Failed to execute request with id %v: %v\n", bookingRequest.RequestId, err)
	}
}

func buildBookingRequestsForUser(userIndex int, params BaselineBookingRequestsParameters, hotelSeed *int, weekSeed *int) []model.BookingRequest {
	var bookingRequests []model.BookingRequest
	for range params.RequestsPerUser {
		*weekSeed++
		if *weekSeed >= params.ActiveWeeksPerHotelCount {
			*weekSeed = 0
			*hotelSeed++
		}
		if *hotelSeed >= params.ActiveHotelsCount {
			*hotelSeed = 0
		}

		userId := "User/" + strconv.Itoa(userIndex)
		hotelId := "Hotel/" + strconv.Itoa(*hotelSeed)
		requestId := userId + "#" + hotelId + ":" + strconv.FormatInt(rand.Int64(), 16)
		weekId := strconv.Itoa(*weekSeed)
		dayOfWeek := rand.IntN(7)
		salt := rand.IntN(100)
		roomType := model.STANDARD
		if salt%2 == 0 {
			roomType = model.PREMIUM
		}
		bookingRequests = append(bookingRequests, model.BookingRequest{
			RequestId: requestId,
			UserId:    userId,
			HotelId:   hotelId,
			RoomType:  roomType,
			BookingPeriod: model.BookingPeriod{
				Week:      weekId,
				DayOfWeek: dayOfWeek,
			},
		})

	}

	return bookingRequests
}

type RequestSender[R any, S any] interface {
	Send(request R) (S, error)
}

type MockRequestSender struct {
}

func (mrs *MockRequestSender) Send(request model.BookingRequest) (model.BookingResponse, error) {
	return model.BookingResponse{
		RequestId:     request.RequestId,
		Success:       true,
		FailureReason: "",
		Reservation:   model.ReservationOverview{},
	}, nil
}

type HotelServiceSender struct {
	HotelService *services.ReservationService
}

func NewServiceSender(hotelService *services.ReservationService) *HotelServiceSender {
	return &HotelServiceSender{HotelService: hotelService}
}

func (s *HotelServiceSender) Send(request model.BookingRequest) (model.BookingResponse, error) {
	return s.HotelService.ReserveRoom(request)
}

type LambdaBaselineHotelSender struct {
	lambdaClient *lambda.Client
}

func NewLambdaBaselineHotelSender(lambdaClient *lambda.Client) *LambdaBaselineHotelSender {
	return &LambdaBaselineHotelSender{lambdaClient: lambdaClient}
}

func (lhs *LambdaBaselineHotelSender) Send(request model.BookingRequest) (model.BookingResponse, error) {
	return lambdautils.InvokeBaselineUserServiceSync(lhs.lambdaClient, request)
}

type ResponseOverview struct {
	Id        string
	StartTime time.Time
	EndTime   time.Time
}

type BaselineBookingRequestsParameters struct {
	ActiveHotelsCount        int
	ActiveWeeksPerHotelCount int
	ActiveUsersCount         int
	RequestsPerUser          int

	SendingPeriodMillis   int
	MaxConcurrentRequests int
}
