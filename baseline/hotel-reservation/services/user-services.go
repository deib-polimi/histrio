package services

import (
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"log"
	"main/baseline/hotel-reservation/model"
	"main/lambdautils"
)

type UserService struct {
	userDao model.UserDao
}

func NewUserService(userDao model.UserDao) *UserService {
	return &UserService{userDao: userDao}
}

func (us *UserService) Book(client *lambda.Client, bookingRequest model.BookingRequest) (model.BookingResponse, error) {
	bookingResponse, err := lambdautils.InvokeBaselineHotelServiceSync(client, bookingRequest)

	if err != nil {
		return model.BookingResponse{}, err
	}

	if bookingResponse.Success {
		err = us.userDao.IncrementTotalReservations(bookingRequest.UserId)
		if err != nil {
			log.Printf("Failed to increment reservation count for user %v: %v\n", bookingRequest.UserId, err)
		}
	} else {
		err = us.userDao.IncrementTotalFailedReservations(bookingRequest.UserId)
		if err != nil {
			log.Printf("Failed to increment failed reservation count for user %v: %v\n", bookingRequest.UserId, err)
		}
	}

	return bookingResponse, nil
}
