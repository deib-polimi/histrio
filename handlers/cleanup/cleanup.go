package main

import (
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func hello() error {
	_, err := dynamoutils.DeleteTable(client, "ActorState")
	_, err = dynamoutils.DeleteTable(client, "ActorInbox")
	_, err = dynamoutils.DeleteTable(client, "ActorTask")
	return err
}
func main() {
	lambda.Start(hello)
}
