package model

import (
	"errors"
	"log"
)

type RoomType string

const (
	STANDARD RoomType = "STANDARD"
	PREMIUM  RoomType = "PREMIUM"
)

type BookingRequest struct {
	RequestId string
	UserId    string
	HotelId   string
	RoomType  RoomType

	BookingPeriod BookingPeriod
}

type BookingResponse struct {
	RequestId     string
	Success       bool
	FailureReason string
	Reservation   ReservationOverview
}

type ReservationOverview struct {
	Id         string
	UserId     string
	HotelId    string
	RoomNumber string

	BookingPeriod BookingPeriod
}

type BookingPeriod struct {
	Week      string
	DayOfWeek int
}

type Hotel struct {
	Id                      string
	TotalReservationsCount  int
	FailedReservationsCount int
}

type WeekAvailability struct {
	WeekId         string
	AvailableRooms map[int]map[RoomType]map[string]struct{}

	TotalRoomsAvailable int
}

func NewWeekAvailability(weekId string, availableRooms map[int]map[RoomType]map[string]struct{}) *WeekAvailability {
	weekAvailability := &WeekAvailability{WeekId: weekId, AvailableRooms: availableRooms}
	i := 0
	for day := range availableRooms {
		for dayType := range day {
			for range dayType {
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
