package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"main/baseline/hotel-reservation/db"
	"main/baseline/hotel-reservation/model"
	"main/baseline/hotel-reservation/services"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) (model.BookingResponse, error) {
	bookingRequest := &model.BookingRequest{}
	err := json.Unmarshal(evt, bookingRequest)

	if err != nil {
		return model.BookingResponse{}, err
	}
	hotelDao := db.NewHotelDynDao(client, uuid.NewString())
	reservationService := services.NewReservationService(hotelDao)
	return reservationService.ReserveRoom(*bookingRequest)
}

func main() {
	lambda.Start(handler)
}
