package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) (bool, error) {
	resultCount, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
		TableName: aws.String("ActorTask"),
		Limit:     aws.Int32(1),
	})

	if err != nil {
		return false, err
	}

	if resultCount.Count == 0 {
		return true, nil
	}

	return false, nil
}

func main() {
	lambda.Start(handler)
}
