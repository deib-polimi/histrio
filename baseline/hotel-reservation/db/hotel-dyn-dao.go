package db

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"main/baseline/hotel-reservation/model"
)

type HotelDynDao struct {
	client             *dynamodb.Client
	functionInstanceId string
}

func NewHotelDynDao(client *dynamodb.Client, functionInstanceId string) *HotelDynDao {
	return &HotelDynDao{client: client, functionInstanceId: functionInstanceId}
}

func (dao *HotelDynDao) GetAndLockWeekAvailability(hotelId string, weekId string) (model.WeekAvailability, error) {
	response, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: hotelId},
			"SK": &types.AttributeValueMemberS{Value: "WeekAvailability#" + weekId},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":myFunctionInstanceId":   &types.AttributeValueMemberS{Value: dao.functionInstanceId},
			":nullFunctionInstanceId": &types.AttributeValueMemberS{Value: "NULL"},
		},
		ConditionExpression: aws.String("locked_instance_id = :nullFunctionInstanceId OR locked_instance_id = :myFunctionInstanceId"),
		UpdateExpression:    aws.String("SET locked_instance_id = :myFunctionInstanceId"),
		ReturnValues:        types.ReturnValueAllNew,
	})

	if err != nil {
		return model.WeekAvailability{}, nil
	}

	weekAvailabilityJson := response.Attributes["current_state"].(*types.AttributeValueMemberS).Value

	weekAvailability := model.WeekAvailability{}

	err = json.Unmarshal([]byte(weekAvailabilityJson), &weekAvailability)

	if err != nil {
		return model.WeekAvailability{}, nil
	}

	return weekAvailability, nil

}

func (dao *HotelDynDao) UnlockWeekAvailability(hotelId string, weekId string) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: hotelId},
			"SK": &types.AttributeValueMemberS{Value: "WeekAvailability#" + weekId},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":myFunctionInstanceId":   &types.AttributeValueMemberS{Value: dao.functionInstanceId},
			":nullFunctionInstanceId": &types.AttributeValueMemberS{Value: "NULL"},
		},
		ConditionExpression: aws.String("locked_instance_id = :nullFunctionInstanceId OR locked_instance_id = :myFunctionInstanceId"),
		UpdateExpression:    aws.String("SET locked_instance_id = :nullFunctionInstanceId"),
		ReturnValues:        types.ReturnValueAllNew,
	})

	return err
}

func (dao *HotelDynDao) IncrementHotelFailedReservations(hotelId string) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: hotelId},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("ADD hotel_failed_reservations :one"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}

func (dao *HotelDynDao) IncrementHotelReservations(hotelId string) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: hotelId},
			"SK": &types.AttributeValueMemberS{Value: "Info"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":one": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("ADD hotel_reservations :one"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}

func (dao *HotelDynDao) UpdateWeekAvailability(hotelId string, weekAvailability model.WeekAvailability) error {
	jsonWeekAvailability, serializationErr := json.Marshal(weekAvailability)

	if serializationErr != nil {
		return serializationErr
	}
	serializedWeekAvailability := string(jsonWeekAvailability)
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("BaselineTable"),
		Key: map[string]types.AttributeValue{
			"PK": &types.AttributeValueMemberS{Value: hotelId},
			"SK": &types.AttributeValueMemberS{Value: "WeekAvailability#" + weekAvailability.WeekId},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":newState": &types.AttributeValueMemberS{Value: serializedWeekAvailability},
		},
		UpdateExpression: aws.String("SET current_state = :newState"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}
