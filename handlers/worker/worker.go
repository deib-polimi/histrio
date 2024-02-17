package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
	"main/worker/infrastructure"
	"main/worker/plugins"
	"net/http"
)

var client *dynamodb.Client
var httpClient *http.Client

func init() {
	client = dynamoutils.CreateAwsClient()
	httpClient = &http.Client{}
}

func handler(_ context.Context, evt json.RawMessage) error {

	workerParameters := &infrastructure.WorkerParameters{}
	err := json.Unmarshal(evt, workerParameters)
	if err != nil {
		return err
	}

	if !infrastructure.IsWorkerParametersValid(workerParameters) {
		return errors.New("worker parameters are not valid")
	}

	timestampCollectorFactory := plugins.NewTimestampCollectorFactoryImpl(httpClient, workerParameters.BaseClockSynchronizerUrl)
	worker := infrastructure.BuildNewWorker(workerParameters, client, timestampCollectorFactory)

	worker.Run()

	return err
}

func main() {
	lambda.Start(handler)
}
