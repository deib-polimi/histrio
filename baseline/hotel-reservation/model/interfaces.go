package model

type HotelDao interface {
	GetAndLockWeekAvailability(hotelId string, weekId string) (WeekAvailability, error)
	UpdateWeekAvailability(hotelId string, weekAvailability WeekAvailability) error
	UnlockWeekAvailability(hotelId string, weekId string) error
	IncrementHotelFailedReservations(hotelId string) error
	IncrementHotelReservations(hotelId string) error
}

type UserDao interface {
	IncrementTotalReservations(userId string) error
	IncrementTotalFailedReservations(userId string) error
}
