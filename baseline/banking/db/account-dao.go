package db

import (
	"context"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/baseline/banking/model"
	"strconv"
	"time"
)

type AccountDynDao struct {
	client             *dynamodb.Client
	functionInstanceId string
}

func NewAccountDynDao(client *dynamodb.Client, functionInstanceId string) *AccountDynDao {
	return &AccountDynDao{client: client, functionInstanceId: functionInstanceId}
}

func (dao *AccountDynDao) GetAndLockAccount(iban string) (model.Account, error) {
	response, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: iban},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":myFunctionInstanceId":   &types.AttributeValueMemberS{Value: dao.functionInstanceId},
			":nullFunctionInstanceId": &types.AttributeValueMemberS{Value: "NULL"},
			":lockExpiration":         &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().Add(time.Duration(30)*time.Second).UnixMilli(), 10)},
			":nowTime":                &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)},
		},
		ConditionExpression:                 aws.String("locked_instance_id = :nullFunctionInstanceId OR locked_instance_id = :myFunctionInstanceId OR attribute_not_exists(lock_expiration) OR :nowTime > lock_expiration"),
		UpdateExpression:                    aws.String("SET locked_instance_id = :myFunctionInstanceId, lock_expiration = :lockExpiration"),
		ReturnValues:                        types.ReturnValueAllOld,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})

	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			log.Printf("I (token=%v) could not lock the account %v: already present token was %v\n", dao.functionInstanceId, iban, condErr.Item["locked_instance_id"].(*types.AttributeValueMemberS).Value)
		} else {
			log.Printf("Dynamodb error that was not a conditional check failed exception\n")
		}
		return model.Account{}, err
	} else {
		log.Printf("I (token=%v) locked account %v that had the token %v", dao.functionInstanceId, iban, response.Attributes["locked_instance_id"].(*types.AttributeValueMemberS).Value)
	}

	amountString := response.Attributes["amount"].(*types.AttributeValueMemberN).Value

	amount, err := strconv.Atoi(amountString)

	if err != nil {
		return model.Account{}, err
	}

	return model.Account{Iban: iban, Amount: amount}, nil
}

func (dao *AccountDynDao) UnlockAccount(iban string) error {
	response, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: iban},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":myFunctionInstanceId":   &types.AttributeValueMemberS{Value: dao.functionInstanceId},
			":nullFunctionInstanceId": &types.AttributeValueMemberS{Value: "NULL"},
		},
		ConditionExpression:                 aws.String("locked_instance_id = :nullFunctionInstanceId OR locked_instance_id = :myFunctionInstanceId"),
		UpdateExpression:                    aws.String("SET locked_instance_id = :nullFunctionInstanceId"),
		ReturnValues:                        types.ReturnValueAllNew,
		ReturnValuesOnConditionCheckFailure: types.ReturnValuesOnConditionCheckFailureAllOld,
	})

	if err != nil {
		var condErr *types.ConditionalCheckFailedException
		if errors.As(err, &condErr) {
			log.Printf("I (token=%v) could not unlock the account %v: already present token was %v\n", dao.functionInstanceId, iban, condErr.Item["locked_instance_id"].(*types.AttributeValueMemberS).Value)
		} else {
			log.Printf("Dynamodb error that was not a conditional check failed exception\n")
		}
	} else {
		log.Printf("I (token=%v) unlocked the account %v. New token: %v\n", dao.functionInstanceId, iban, response.Attributes["locked_instance_id"].(*types.AttributeValueMemberS).Value)
	}

	return err
}

func (dao *AccountDynDao) UpdateAccount(account model.Account) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: account.Iban},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":amount": &types.AttributeValueMemberN{Value: strconv.Itoa(account.Amount)},
		},
		UpdateExpression: aws.String("SET amount = :amount"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}
