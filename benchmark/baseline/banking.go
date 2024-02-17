package baseline

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
)

func LoadBaselineBankingState(params BaselineBankingParameters, client *dynamodb.Client) error {
	return dynamoutils.AddBaselineAccountsBatch(client, params.AccountsCount)
}

type BaselineBankingParameters struct {
	AccountsCount int
}
