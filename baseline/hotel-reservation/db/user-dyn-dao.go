package db

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type UserDynDao struct {
	client *dynamodb.Client
}

func NewUserDynDao(client *dynamodb.Client) *UserDynDao {
	return &UserDynDao{client: client}
}

func (dao *UserDynDao) IncrementTotalReservations(userId string) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: userId},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("ADD total_reservations :one"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}

func (dao *UserDynDao) IncrementTotalFailedReservations(userId string) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: userId},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("ADD total_failed_reservations :one"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}
