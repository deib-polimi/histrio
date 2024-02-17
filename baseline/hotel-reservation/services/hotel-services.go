package services

import (
	"log"
	"main/baseline/hotel-reservation/model"
	"main/utils"
)

type ReservationService struct {
	hotelDao model.HotelDao
}

func NewReservationService(hotelDao model.HotelDao) *ReservationService {
	return &ReservationService{hotelDao: hotelDao}
}

func (rs *ReservationService) ReserveRoom(bookingRequest model.BookingRequest) (model.BookingResponse, error) {

	r := utils.NewDefaultRetrier[model.WeekAvailability]()
	weekAvailability, err := r.DoWithReturn(func() (model.WeekAvailability, error) {
		return rs.hotelDao.GetAndLockWeekAvailability(bookingRequest.HotelId, bookingRequest.BookingPeriod.Week)
	})

	if err != nil {
		return model.BookingResponse{}, err
	}

	defer func() {
		unlockRetrier := utils.NewDefaultRetrier[struct{}]()
		_, unlockErr := unlockRetrier.DoWithReturn(func() (struct{}, error) {
			innerErr := rs.hotelDao.UnlockWeekAvailability(bookingRequest.HotelId, bookingRequest.BookingPeriod.Week)
			return struct{}{}, innerErr
		})

		if unlockErr != nil {
			log.Fatalf("Could not unlock week availability %v for hotel %v: %v", bookingRequest.BookingPeriod.Week, bookingRequest.HotelId, unlockErr)
		}
	}()

	roomId, err := weekAvailability.ReserveRoom(bookingRequest.RoomType, bookingRequest.BookingPeriod.DayOfWeek)
	var response model.BookingResponse
	if err != nil {
		incrementErr := rs.hotelDao.IncrementHotelFailedReservations(bookingRequest.HotelId)
		if incrementErr != nil {
			log.Printf("Failed to increment reservation count for hotel %v: %v\n", bookingRequest.HotelId, incrementErr)
		}

		response = model.BookingResponse{
			RequestId:     bookingRequest.RequestId,
			Success:       false,
			FailureReason: "There was no enough rooms for hotel " + bookingRequest.HotelId + " in the selected period",
		}

	} else {
		incrementErr := rs.hotelDao.IncrementHotelReservations(bookingRequest.HotelId)
		if incrementErr != nil {
			log.Printf("Failed to increment reservation count for hotel %v : %v\n", bookingRequest.HotelId, incrementErr)
		}
		reservationOverview := model.ReservationOverview{
			Id:            bookingRequest.RequestId,
			UserId:        bookingRequest.UserId,
			HotelId:       bookingRequest.HotelId,
			RoomNumber:    roomId,
			BookingPeriod: bookingRequest.BookingPeriod,
		}
		response = model.BookingResponse{
			RequestId:   bookingRequest.RequestId,
			Success:     true,
			Reservation: reservationOverview,
		}

		updateErr := rs.hotelDao.UpdateWeekAvailability(bookingRequest.HotelId, weekAvailability)
		if updateErr != nil {
			log.Printf("Failed to update week availbaility %v for hotel %v: %v\n", weekAvailability.WeekId, bookingRequest.HotelId, updateErr)
		}
	}

	return response, nil

}
