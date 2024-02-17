package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	lambdaservice "github.com/aws/aws-sdk-go-v2/service/lambda"
	"main/baseline/hotel-reservation/db"
	"main/baseline/hotel-reservation/model"
	"main/baseline/hotel-reservation/services"
	"main/dynamoutils"
	"main/lambdautils"
)

var client *dynamodb.Client
var lambdaClient *lambdaservice.Client

func init() {
	client = dynamoutils.CreateAwsClient()
	lambdaClient = lambdautils.CreateNewClient()
}

func handler(_ context.Context, evt json.RawMessage) (model.BookingResponse, error) {
	bookingRequest := &model.BookingRequest{}
	err := json.Unmarshal(evt, bookingRequest)

	if err != nil {
		return model.BookingResponse{}, err
	}
	userDao := db.NewUserDynDao(client)
	userService := services.NewUserService(userDao)
	return userService.Book(lambdaClient, *bookingRequest)
}

func main() {
	lambda.Start(handler)
}
