package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/benchmark/sut"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) error {
	parameters := &sut.HotelReservationParameters{}
	err := json.Unmarshal(evt, parameters)
	if err != nil {
		return err
	}

	err = sut.HotelReservationLoadInboxesAndTasks(parameters, client)

	if err != nil {
		return err
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
