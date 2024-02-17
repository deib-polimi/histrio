package dyndao

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/utils"
	"main/worker/domain"
	"reflect"
)

type DynPhysicalPartitionManagerDao struct {
	Client   *dynamodb.Client
	workerId string

	alreadyReadMessages utils.Set[domain.MessageIdentifier]
	entityLoader        *domain.EntityLoader
}

func NewDynPhysicalPartitionManagerDao(client *dynamodb.Client, workerId string, entityLoader *domain.EntityLoader) *DynPhysicalPartitionManagerDao {
	return &DynPhysicalPartitionManagerDao{
		Client:              client,
		workerId:            workerId,
		alreadyReadMessages: utils.NewMapSet[domain.MessageIdentifier](),
		entityLoader:        entityLoader,
	}
}

func (dao *DynPhysicalPartitionManagerDao) FetchNewMessagesFromInbox(phyPartitionId domain.PhysicalPartitionId) (inbox []domain.ActorMessage, err error) {
	var actorInbox []domain.ActorMessage

	inboxResponse, err := dao.Client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String("ActorInbox"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":phyPartitionId": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
		KeyConditionExpression: aws.String("phy_partition_id = :phyPartitionId"),
		Limit:                  aws.Int32(500),
		Select:                 types.SelectAllAttributes,
	})

	if err != nil {
		return []domain.ActorMessage{}, err
	}

	for _, item := range inboxResponse.Items {
		uniqueTimestamp := item["timestamp"].(*types.AttributeValueMemberS).Value

		senderId, parsingErr := domain.StrToActorId(item["sender_id"].(*types.AttributeValueMemberS).Value)
		if parsingErr != nil {
			log.Printf("ERROR: could not parse the sender id '%v'\n", item["sender_id"].(*types.AttributeValueMemberS).Value)
			continue
		}

		serializedContent := item["content"].(*types.AttributeValueMemberS).Value
		messageTypeName := item["type"].(*types.AttributeValueMemberS).Value
		messageType, typeNotRegisterErr := dao.entityLoader.GetTypeByName(messageTypeName)
		if typeNotRegisterErr != nil {
			log.Fatalf("Could not find the correct type while deserializing message. Error: %v\n", typeNotRegisterErr)
			return nil, typeNotRegisterErr
		}

		receiverId, parsingErr := domain.StrToActorId(item["receiver_id"].(*types.AttributeValueMemberS).Value)
		if parsingErr != nil {
			log.Printf("ERROR: could not parse the receiver id '%v'\n", item["receiver_id"].(*types.AttributeValueMemberS).Value)
			continue
		}
		messageIdentifier := domain.MessageIdentifier{ActorId: receiverId, UniqueTimestamp: uniqueTimestamp}

		deserializedMessage := reflect.New(messageType).Interface()

		parsingErr = json.Unmarshal([]byte(serializedContent), deserializedMessage)
		if parsingErr != nil {
			return nil, parsingErr
		}

		if !dao.alreadyReadMessages.Contains(messageIdentifier) {
			actorInbox = append(actorInbox, domain.ActorMessage{
				Id:       messageIdentifier,
				SenderId: senderId,
				Content:  deserializedMessage,
			})
			dao.alreadyReadMessages.Add(messageIdentifier)
		}

	}

	return actorInbox, nil
}

func (dao *DynPhysicalPartitionManagerDao) SealPhysicalPartition(phyPartitionId domain.PhysicalPartitionId) error {
	return dao.changeSealAttribute(phyPartitionId, true)
}

func (dao *DynPhysicalPartitionManagerDao) UnsealPhysicalPartition(phyPartitionId domain.PhysicalPartitionId) error {
	return dao.changeSealAttribute(phyPartitionId, false)
}

func (dao *DynPhysicalPartitionManagerDao) DeleteActorTask(phyPartitionId domain.PhysicalPartitionId) error {
	_, err := dao.Client.DeleteItem(context.TODO(), &dynamodb.DeleteItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
	})

	return err
}

func (dao *DynPhysicalPartitionManagerDao) ForgetMessage(identifier domain.MessageIdentifier) {
	dao.alreadyReadMessages.Remove(identifier)
}

func (dao *DynPhysicalPartitionManagerDao) ReleasePhyPartition(phyPartitionId domain.PhysicalPartitionId, workerId string) error {
	_, err := dao.Client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":workerId":   &types.AttributeValueMemberS{Value: workerId},
			":nullWorker": &types.AttributeValueMemberS{Value: "NULL"},
		},
		ConditionExpression: aws.String("worker_id = :workerId"),
		UpdateExpression:    aws.String("SET worker_id = :nullWorker"),
		ReturnValues:        types.ReturnValueAllNew,
	})

	return err
}

func (dao *DynPhysicalPartitionManagerDao) changeSealAttribute(phyPartitionId domain.PhysicalPartitionId, isSealed bool) error {
	_, err := dao.Client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":sealed": &types.AttributeValueMemberBOOL{Value: isSealed},
		},
		UpdateExpression: aws.String("SET is_sealed = :sealed"),
	})

	return err
}
