package request_sender

import (
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"log"
	"main/baseline/hotel-reservation/model"
	"main/baseline/hotel-reservation/services"
	"main/benchmark"
	"main/lambdautils"
	"math/rand"
	"strconv"
	"sync"
	"time"
)

func SendAndMeasureBaselineBookingRequests(
	params BaselineBookingRequestsParameters,
	sender RequestSender[model.BookingRequest, model.BookingResponse],
	timeLogger *benchmark.RequestTimeLoggerImpl) {

	var requestSenderWg sync.WaitGroup

	timeLogger.Start()
	defer timeLogger.Stop()

	inputQueue := make(chan model.BookingRequest, params.MaxConcurrentRequests)
	for range params.MaxConcurrentRequests {
		requestSenderWg.Add(1)
		go handleBookingRequest(inputQueue, &requestSenderWg, sender, timeLogger)
	}

	hotelSeed := 0
	weekSeed := 0
	for i := range params.ActiveUsersCount {
		for j, bookingRequest := range buildBookingRequestsForUser(i, params, &hotelSeed, &weekSeed) {
			if j%params.MaxConcurrentRequests == 0 && params.SendingPeriodMillis != -1 {
				time.Sleep(time.Duration(params.SendingPeriodMillis) * time.Millisecond)
			}
			inputQueue <- bookingRequest
		}
	}
	close(inputQueue)
	requestSenderWg.Wait()

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
		weekId := strconv.Itoa(*weekSeed)
		dayOfWeek := rand.Intn(7)
		salt := rand.Intn(100)
		roomType := model.STANDARD
		if salt%2 == 0 {
			roomType = model.PREMIUM
		}
		bookingRequests = append(bookingRequests, model.BookingRequest{
			RequestId: userId + "->" + hotelId + "->" + weekId + "->" + strconv.Itoa(dayOfWeek) + "#" + strconv.Itoa(rand.Intn(100)),
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

func handleBookingRequest(inputChannel chan model.BookingRequest, wg *sync.WaitGroup,
	requestSender RequestSender[model.BookingRequest, model.BookingResponse],
	timeLogger *benchmark.RequestTimeLoggerImpl) {
	for bookingRequest := range inputChannel {
		err := timeLogger.LogStartRequest(bookingRequest.RequestId)
		if err != nil {
			log.Printf("Could not log the start request %v: %v\n", bookingRequest.RequestId, err)
		}
		_, err = requestSender.Send(bookingRequest)
		if err != nil {
			log.Printf("Failed to execute request with id %v: %v\n", bookingRequest.RequestId, err)
		}
		err = timeLogger.LogEndRequest(bookingRequest.RequestId)
		if err != nil {
			log.Printf("Could not log the end request %v: %v\n", bookingRequest.RequestId, err)
		}
	}
	wg.Done()
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
