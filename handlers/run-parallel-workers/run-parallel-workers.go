package main

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-lambda-go/lambda"
	lambdaservice "github.com/aws/aws-sdk-go-v2/service/lambda"
	"main/lambdautils"
	"main/worker/infrastructure"
	"strconv"
)

var lambdaClient *lambdaservice.Client

func init() {
	lambdaClient = lambdautils.CreateNewClient()
}

func handler(_ context.Context, evt json.RawMessage) error {
	parallelWorkersInput := &ParallelWorkersInput{}
	err := json.Unmarshal(evt, parallelWorkersInput)
	if err != nil {
		return err
	}

	parallelWorkersCount := parallelWorkersInput.ParallelWorkersCount

	if parallelWorkersCount <= 0 {
		return errors.New("cannot have a non positive number of parallel workers")
	}

	var workerParamsList []infrastructure.WorkerParameters

	for i := range parallelWorkersCount {
		workerParameters := parallelWorkersInput.WorkerParams
		workerParameters.WorkerId = "Worker-" + strconv.Itoa(i)
		workerParameters.RunId = parallelWorkersInput.RunId
		if !infrastructure.IsWorkerParametersValid(&workerParameters) {
			return errors.New("worker parameters are not valid")
		}
		workerParamsList = append(workerParamsList, workerParameters)
	}

	for i := range parallelWorkersCount {
		err = lambdautils.InvokeWorkerAsync(lambdaClient, workerParamsList[i])
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	lambda.Start(handler)
}

type ParallelWorkersInput struct {
	WorkerParams         infrastructure.WorkerParameters
	ParallelWorkersCount int
	RunId                string
}
