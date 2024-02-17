package domain

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type Actor interface {
	ReceiveMessage(message Message) error
	GetId() ActorId
	SetId(ActorId)
}

type ActorId struct {
	InstanceId     string
	PhyPartitionId PhysicalPartitionId
}

func (a ActorId) String() string {
	return a.PhyPartitionId.PartitionName + "/" + a.PhyPartitionId.PhysicalPartitionName + "/" + a.InstanceId
}

func (a ActorId) MarshalJSON() ([]byte, error) {
	return json.Marshal(a.String())
}

func (a *ActorId) UnmarshalJSON(data []byte) error {
	var strRepr string
	err := json.Unmarshal(data, &strRepr)
	if err != nil {
		return err
	}
	actorId, err := StrToActorId(strRepr)
	if err != nil {
		return err
	}
	a.InstanceId = actorId.InstanceId
	a.PhyPartitionId = actorId.PhyPartitionId
	return nil
}

func StrToActorId(id string) (ActorId, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 3 {
		return ActorId{}, errors.New("could not parse an actod id from '" + id + "'")
	}

	return ActorId{
		PhyPartitionId: PhysicalPartitionId{
			PartitionName:         parts[0],
			PhysicalPartitionName: parts[1],
		},
		InstanceId: parts[2],
	}, nil
}

type CorrelationInfo struct {
	Id      string
	Samples []Sample
}

func NewCorrelationInfo(id string, samples []Sample) *CorrelationInfo {
	return &CorrelationInfo{Id: id, Samples: samples}
}

type Sample struct {
	Name      string
	StartTime int64
	EndTime   int64
}

func NewSample(name string, startTime time.Time, duration time.Duration) *Sample {
	return &Sample{Name: name, StartTime: startTime.UnixMilli(), EndTime: startTime.Add(duration).UnixMilli()}
}

// message types

type SimpleMessage struct {
	Content string
}

// implements Actor

type MyActor struct {
	Id            ActorId
	CurrentState  string
	MessageSender MessageSender
	ActorSpawner  ActorSpawner
}

func (a *MyActor) ReceiveMessage(message Message) error {
	simpleMessage := message.(*SimpleMessage)
	a.CurrentState = a.CurrentState + simpleMessage.Content

	if simpleMessage.Content != "END" {
		destination := ActorId{
			InstanceId: a.Id.InstanceId + "-test",
			PhyPartitionId: PhysicalPartitionId{
				PartitionName:         a.Id.PhyPartitionId.PartitionName,
				PhysicalPartitionName: a.Id.PhyPartitionId.PhysicalPartitionName + "-test",
			},
		}

		if simpleMessage.Content == "SPAWN" {
			_, err := a.ActorSpawner.Spawn(&MyActor{}, a.Id.PhyPartitionId.PartitionName, "my-test-spawned-actor")
			if err != nil {
				return err
			}
			return nil
		}

		a.MessageSender.Tell(SimpleMessage{Content: "END"}, destination)

	}

	return nil
}

func (a *MyActor) GetId() ActorId {
	return a.Id
}

func (a *MyActor) SetId(id ActorId) {
	a.Id = id
}

type SinkActor struct {
	Id ActorId
}

func (s *SinkActor) ReceiveMessage(message Message) error {
	return nil
}

func (s *SinkActor) GetId() ActorId {
	return s.Id
}

func (s *SinkActor) SetId(id ActorId) {
	s.Id = id
}

//----------------------------------------------------------------------------------------------------------------------
//	HOTEL RESERVATION USE CASE
//----------------------------------------------------------------------------------------------------------------------

type RoomType string

const (
	STANDARD RoomType = "STANDARD"
	PREMIUM  RoomType = "PREMIUM"
)

type BookingRequest struct {
	RequestId string
	UserId    ActorId
	HotelId   ActorId
	RoomType  RoomType

	BookingPeriod BookingPeriod

	CorrelationInfo CorrelationInfo
}

type BookingPeriod struct {
	Week      string
	DayOfWeek int
}

type BookingResponse struct {
	RequestId     string
	Success       bool
	FailureReason string
	Reservation   ReservationOverview

	CorrelationInfo CorrelationInfo
}

type ReservationOverview struct {
	Id         string
	UserId     ActorId
	HotelId    ActorId
	RoomNumber string

	BookingPeriod BookingPeriod
}

type HotelReservation struct {
	Id string

	UserId        ActorId
	RoomType      RoomType
	BookingPeriod BookingPeriod
}

func (r *HotelReservation) GetId() string {
	return r.Id
}

func (r *HotelReservation) GetQueryableAttributes() map[string]string {
	return make(map[string]string)
}

// Hotel

type Hotel struct {
	Id                      ActorId
	WeekAvailabilities      QueryableCollection[*WeekAvailability]
	TotalReservationsCount  int
	FailedReservationsCount int

	MessageSender MessageSender
}

func NewHotel(id ActorId) *Hotel {
	return &Hotel{Id: id}
}

func (h *Hotel) ReceiveMessage(message Message) error {
	bookingRequest := message.(*BookingRequest)
	return h.onBookingRequest(*bookingRequest)
}

func (h *Hotel) onBookingRequest(bookingRequest BookingRequest) error {
	weekAvailability, err := h.WeekAvailabilities.Get(bookingRequest.BookingPeriod.Week)
	if err != nil {
		return err
	}

	roomId, err := weekAvailability.ReserveRoom(bookingRequest.RoomType, bookingRequest.BookingPeriod.DayOfWeek)
	if err != nil {
		h.FailedReservationsCount++
		h.MessageSender.Tell(BookingResponse{
			RequestId:     bookingRequest.RequestId,
			Success:       false,
			FailureReason: "There was no enough rooms for hotel " + h.GetId().String() + " in the selected period",
		}, bookingRequest.UserId)
	} else {
		h.TotalReservationsCount++
		reservation := ReservationOverview{
			Id:            bookingRequest.RequestId,
			UserId:        bookingRequest.UserId,
			HotelId:       h.GetId(),
			RoomNumber:    roomId,
			BookingPeriod: bookingRequest.BookingPeriod,
		}

		h.MessageSender.Tell(BookingResponse{
			RequestId:   bookingRequest.RequestId,
			Success:     true,
			Reservation: reservation,
		}, bookingRequest.UserId)
	}

	return nil
}

func (h *Hotel) GetId() ActorId {
	return h.Id
}

func (h *Hotel) SetId(id ActorId) {
	h.Id = id
}

type WeekAvailability struct {
	WeekId         string
	AvailableRooms map[int]map[RoomType]map[string]struct{}

	TotalRoomsAvailable int
}

func NewWeekAvailability(weekId string, availableRooms map[int]map[RoomType]map[string]struct{}) *WeekAvailability {
	weekAvailability := &WeekAvailability{WeekId: weekId, AvailableRooms: availableRooms}
	i := 0
	for _, roomTypes := range availableRooms {
		for _, rooms := range roomTypes {
			for range len(rooms) {
				i++
			}
		}
	}
	weekAvailability.TotalRoomsAvailable = i
	return weekAvailability
}

func (wa *WeekAvailability) ReserveRoom(roomType RoomType, dayOfWeek int) (string, error) {
	roomsForDay, ok := wa.AvailableRooms[dayOfWeek]
	if !ok {
		log.Fatalf("Week availability malformed: cannot find the day %v in the week availabiliy\n", dayOfWeek)
	}

	roomsForDayAndType, ok := roomsForDay[roomType]

	if !ok {
		log.Fatalf("Week availability malformed: cannot find the room type %v\n", roomType)
	}

	if len(roomsForDayAndType) == 0 {
		return "", errors.New("no more " + string(roomType) + " rooms")
	}

	var pickedRoomId string
	for roomId := range roomsForDayAndType {
		pickedRoomId = roomId
		delete(roomsForDayAndType, roomId)
		wa.TotalRoomsAvailable--
		break
	}

	return pickedRoomId, nil
}

func (wa *WeekAvailability) GetId() string {
	return wa.WeekId
}

func (wa *WeekAvailability) GetQueryableAttributes() map[string]string {
	return map[string]string{
		"TotalRoomsAvailable": strconv.Itoa(wa.TotalRoomsAvailable),
	}
}

// User

type User struct {
	Username                ActorId
	TotalReservationsCount  int
	FailedReservationsCount int
	Counter                 int

	MessageSender   MessageSender
	BenchmarkHelper BenchmarkHelper
}

func NewUser() *User {
	return &User{}
}

func (u *User) ReceiveMessage(message Message) error {
	if bookingRequest, ok := message.(*BookingRequest); ok {
		return u.onBookingRequest(*bookingRequest)
	} else if bookingResponse, ok := message.(*BookingResponse); ok {
		return u.onBookingResponse(*bookingResponse)
	} else {
		log.Fatalf("Type '%v' not handled by User actor %v", reflect.TypeOf(message), u.GetId())
	}
	return nil

}

func (u *User) onBookingRequest(bookingRequest BookingRequest) error {
	requestId := u.GetId().String() + "#" + bookingRequest.HotelId.String() + strconv.Itoa(u.Counter)
	u.BenchmarkHelper.StartMeasurement(requestId, "", true)
	u.Counter++
	bookingRequest.RequestId = requestId
	u.MessageSender.Tell(bookingRequest, bookingRequest.HotelId)
	return nil
}

func (u *User) onBookingResponse(bookingResponse BookingResponse) error {
	if bookingResponse.Success == true {
		u.TotalReservationsCount++
	} else {
		u.FailedReservationsCount++
	}

	u.MessageSender.TellExternal(bookingResponse, bookingResponse.RequestId)
	u.BenchmarkHelper.EndMeasurement(bookingResponse.RequestId, "", false)

	return nil
}

func (u *User) GetId() ActorId {
	return u.Username
}

func (u *User) SetId(id ActorId) {
	u.Username = id
}

//----------------------------------------------------------------------------------------------------------------------
//	BANKING USE CASE
//----------------------------------------------------------------------------------------------------------------------

type TransactionRequest struct {
	TransactionId   string
	SourceIban      string
	DestinationIban string
	Amount          int
}

func NewTransactionRequest(srcId string, dstId string, amount int) TransactionRequest {
	return TransactionRequest{
		TransactionId:   "TX" + srcId + "->" + dstId + ":" + strconv.Itoa(amount) + uuid.NewString(),
		SourceIban:      srcId,
		DestinationIban: dstId,
		Amount:          amount,
	}
}

type TransactionResponse struct {
	TransactionId string
	Success       bool
}

type BankBranch struct {
	Id       ActorId
	Accounts QueryableCollection[*Account]

	MessageSender   MessageSender
	BenchmarkHelper BenchmarkHelper
}

func NewBankBranch(id ActorId) *BankBranch {
	return &BankBranch{Id: id}
}

func (bb *BankBranch) ReceiveMessage(message Message) error {
	if transactionRequest, ok := message.(*TransactionRequest); ok {
		return bb.onTransactionRequest(*transactionRequest)
	} else {
		log.Fatalf("Type '%v' not handled by BankBranc actor	%v", reflect.TypeOf(message), bb.GetId())
		return nil
	}
}

func (bb *BankBranch) onTransactionRequest(transactionRequest TransactionRequest) error {
	bb.BenchmarkHelper.StartMeasurement(transactionRequest.TransactionId, "", true)
	srcAccount, err := bb.Accounts.Get(transactionRequest.SourceIban)
	if err != nil {
		return err
	}

	dstAccount, err := bb.Accounts.Get(transactionRequest.DestinationIban)
	if err != nil {
		return err
	}

	if dstAccount.Amount-transactionRequest.Amount < 0 {
		bb.MessageSender.TellExternal(TransactionResponse{
			TransactionId: transactionRequest.TransactionId,
			Success:       false,
		}, transactionRequest.TransactionId)
		bb.BenchmarkHelper.EndMeasurement(transactionRequest.TransactionId, "", false)

		return nil
	}

	dstAccount.Amount -= transactionRequest.Amount
	srcAccount.Amount += transactionRequest.Amount

	bb.MessageSender.TellExternal(TransactionResponse{
		TransactionId: transactionRequest.TransactionId,
		Success:       true,
	}, transactionRequest.TransactionId)
	bb.BenchmarkHelper.EndMeasurement(transactionRequest.TransactionId, "", false)
	return nil
}

func (bb *BankBranch) GetId() ActorId {
	return bb.Id
}

func (bb *BankBranch) SetId(id ActorId) {
	bb.Id = id
}

type Account struct {
	Id     string
	Amount int
}

func (a *Account) GetId() string {
	return a.Id
}

func (a *Account) GetQueryableAttributes() map[string]string {
	return map[string]string{}
}

//----------------------------------------------------------------------------------------------------------------------
//	TRAVEL AGENCY USE CASE
//----------------------------------------------------------------------------------------------------------------------

// messages

type DiscountRequest struct {
	Destination string
	Discount    float64
}

type AddressUpdateRequest struct {
	NewAddress string
}

type TravelBookingRequest struct {
	UserId   ActorId
	TravelId string
}

type TravelBookingReply struct {
	AgencyId       ActorId
	TravelId       string
	IsTravelBooked bool
	FailureReason  string

	TravelAgentId ActorId
}

//Journey agency actor

type TravelAgency struct {
	Id      ActorId
	Address string
	Catalog QueryableCollection[*Journey]

	MessageSender MessageSender
	ActorSpawner  ActorSpawner
}

func (ta *TravelAgency) ReceiveMessage(message Message) error {
	if discountRequest, ok := message.(*DiscountRequest); ok {
		return ta.applyDiscount(*discountRequest)
	} else if addressUpdateRequest, ok := message.(*AddressUpdateRequest); ok {
		return ta.updateAddress(*addressUpdateRequest)
	} else if travelBookingRequest, ok := message.(*TravelBookingRequest); ok {
		return ta.processTravelBookingRequest(*travelBookingRequest)
	} else {
		return errors.New(fmt.Sprintf("Type '%v' not handled by TravelAgency actor %v", reflect.TypeOf(message), ta.GetId()))
	}
}

func (ta *TravelAgency) updateAddress(addressUpdateRequest AddressUpdateRequest) error {
	ta.Address = addressUpdateRequest.NewAddress
	return nil
}

func (ta *TravelAgency) processTravelBookingRequest(travelBooking TravelBookingRequest) error {
	journey, err := ta.Catalog.Get(travelBooking.TravelId)
	if err != nil {
		return err
	}
	response := &TravelBookingReply{
		AgencyId: ta.Id,
		TravelId: travelBooking.TravelId,
	}
	if journey.AvailableBookings == 0 {
		response.IsTravelBooked = false
		response.FailureReason = "No more booking allowed for this journey"
	} else {
		response.IsTravelBooked = true
		travelAgentId, err := ta.ActorSpawner.Spawn(&TravelAgent{}, ta.Id.PhyPartitionId.PartitionName, uuid.NewString())
		if err != nil {
			return err
		}
		response.TravelAgentId = travelAgentId
		journey.AvailableBookings -= 1
	}

	ta.MessageSender.Tell(*response, travelBooking.UserId)
	return nil
}

func (ta *TravelAgency) applyDiscount(discountRequest DiscountRequest) error {
	journeysToUpdate, err := ta.Catalog.Find("Destination", discountRequest.Destination)
	if err != nil {
		return err
	}

	for _, journey := range journeysToUpdate {
		journey.Cost -= journey.Cost * discountRequest.Discount
	}

	return nil
}

func (ta *TravelAgency) GetId() ActorId {
	return ta.Id
}

func (ta *TravelAgency) SetId(actorId ActorId) {
	ta.Id = actorId
}

type Journey struct {
	Id                string
	Destination       string
	Cost              float64
	AvailableBookings int
}

func (t *Journey) GetId() string {
	return t.Id
}

func (t *Journey) GetQueryableAttributes() map[string]string {
	return map[string]string{
		"Destination": t.Destination,
	}
}

type TravelAgent struct {
	Id ActorId
}

func (ta *TravelAgent) ReceiveMessage(message Message) error {
	return nil
}

func (ta *TravelAgent) GetId() ActorId {
	return ta.Id
}

func (ta *TravelAgent) SetId(actorId ActorId) {
	ta.Id = actorId
}
