package lambdautils

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/lambda"
	"log"
	bankingmodel "main/baseline/banking/model"
	"main/baseline/hotel-reservation/model"
	"main/worker/infrastructure"
)

func CreateNewClient() *lambda.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-west-3"),
		config.WithClientLogMode(aws.LogRetries),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := lambda.NewFromConfig(cfg)
	return client
}

func InvokeWorkerAsync(client *lambda.Client, workerParams infrastructure.WorkerParameters) error {
	workerParamsJson, err := json.Marshal(workerParams)

	if err != nil {
		return err
	}
	_, err = client.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName:   aws.String("Worker"),
		InvocationType: "Event",
		Payload:        workerParamsJson,
	})

	return err
}

func InvokeBaselineUserServiceSync(client *lambda.Client, bookingRequest model.BookingRequest) (model.BookingResponse, error) {
	bookingRequestJson, err := json.Marshal(bookingRequest)

	if err != nil {
		return model.BookingResponse{}, err
	}
	response, err := client.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName: aws.String("BaselineUserService"),
		Payload:      bookingRequestJson,
	})

	bookingResponseJson := response.Payload
	var bookingResponse model.BookingResponse

	err = json.Unmarshal(bookingResponseJson, &bookingResponse)

	if err != nil {
		return model.BookingResponse{}, err
	}

	return bookingResponse, nil
}

func InvokeBaselineHotelServiceSync(client *lambda.Client, bookingRequest model.BookingRequest) (model.BookingResponse, error) {
	bookingRequestJson, err := json.Marshal(bookingRequest)

	if err != nil {
		return model.BookingResponse{}, err
	}
	response, err := client.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName: aws.String("BaselineHotelService"),
		Payload:      bookingRequestJson,
	})

	bookingResponseJson := response.Payload
	var bookingResponse model.BookingResponse

	err = json.Unmarshal(bookingResponseJson, &bookingResponse)

	if err != nil {
		return model.BookingResponse{}, err
	}

	return bookingResponse, nil
}

func InvokeBaselineBankingServiceSync(client *lambda.Client, transactionRequest bankingmodel.TransactionRequest) (bankingmodel.TransactionResponse, error) {
	bankingRequestJson, err := json.Marshal(transactionRequest)

	if err != nil {
		return bankingmodel.TransactionResponse{}, err
	}
	response, err := client.Invoke(context.TODO(), &lambda.InvokeInput{
		FunctionName: aws.String("BaselineBankingService"),
		Payload:      bankingRequestJson,
	})

	transactionResponseJson := response.Payload
	var transactionResponse bankingmodel.TransactionResponse

	err = json.Unmarshal(transactionResponseJson, &transactionResponse)

	if err != nil {
		return bankingmodel.TransactionResponse{}, err
	}

	return transactionResponse, nil

}
