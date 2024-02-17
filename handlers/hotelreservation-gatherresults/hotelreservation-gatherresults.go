package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/benchmark"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) error {
	gatherResultsInput := &GatherResultsInput{}
	err := json.Unmarshal(evt, gatherResultsInput)
	if err != nil {
		return err
	}

	runId := gatherResultsInput.RunId
	if runId == "" {
		return errors.New("cannot gather results without specifying the run id")
	}

	err = benchmark.ComputeAndExportBenchmarkResults(client, gatherResultsInput.RunId, benchmark.NewLogExporter(runId+"-long"), benchmark.NewLogExporter(runId+"-short"))

	return err
}

func main() {
	lambda.Start(handler)
}

type GatherResultsInput struct {
	RunId string
}
