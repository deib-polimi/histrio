package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/google/uuid"
	"main/baseline/banking/db"
	"main/baseline/banking/model"
	"main/baseline/banking/services"
	"main/dynamoutils"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) (model.TransactionResponse, error) {
	transactionRequest := &model.TransactionRequest{}
	err := json.Unmarshal(evt, transactionRequest)

	if err != nil {
		return model.TransactionResponse{}, err
	}
	accountDao := db.NewAccountDynDao(client, uuid.NewString())
	bankingService := services.NewBankingService(accountDao)
	return bankingService.ExecuteTransaction(*transactionRequest)
}

func main() {
	lambda.Start(handler)
}
