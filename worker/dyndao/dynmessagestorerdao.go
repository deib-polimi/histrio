package dyndao

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/worker/domain"
	"reflect"
	"strconv"
	"time"
)

type DynMessageStorerDao struct {
	client *dynamodb.Client
}

func NewDynMessageStorerDao(client *dynamodb.Client) *DynMessageStorerDao {
	return &DynMessageStorerDao{client: client}
}

func (dao *DynMessageStorerDao) StoreMessage(payload string, receiver domain.ActorId, uniqueSourceId string, seqNumber int, eventToken string) error {
	_, err := dao.client.PutItem(context.TODO(), dao.buildMessagePutInput(payload, receiver, uniqueSourceId, seqNumber, eventToken))
	return err
}

func (dao *DynMessageStorerDao) AddActorTask(shardId domain.PhysicalPartitionId) error {
	_, err := dao.client.UpdateItem(context.TODO(), dao.buildActorTaskUpdateInput(shardId))
	return err
}

func (dao *DynMessageStorerDao) buildMessagePutInput(payload domain.Message, receiver domain.ActorId, uniqueSourceId string, seqNumber int, eventToken string) *dynamodb.PutItemInput {
	messageId := strconv.FormatInt(time.Now().UnixMilli(), 10) + "#" + uniqueSourceId + "#" + strconv.Itoa(seqNumber)
	messageJson, err := json.Marshal(payload)
	messageType := reflect.TypeOf(payload)

	if err != nil {
		log.Fatalf("Could not serialize message with id %v", messageId)
	}

	return &dynamodb.PutItemInput{
		TableName: aws.String("ActorInbox"),
		Item: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: receiver.PhyPartitionId.String()},
			"timestamp":        &types.AttributeValueMemberS{Value: messageId},
			"receiver_id":      &types.AttributeValueMemberS{Value: receiver.String()},
			"sender_id":        &types.AttributeValueMemberS{Value: eventToken},
			"content":          &types.AttributeValueMemberS{Value: string(messageJson)},
			"type":             &types.AttributeValueMemberS{Value: messageType.Name()},
		},
	}
}

func (dao *DynMessageStorerDao) buildActorTaskUpdateInput(shardId domain.PhysicalPartitionId) *dynamodb.UpdateItemInput {
	return &dynamodb.UpdateItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: shardId.String()},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nullWorker": &types.AttributeValueMemberS{Value: "NULL"},
			":notSealed":  &types.AttributeValueMemberBOOL{Value: false},
			":now":        &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(phy_partition_id) or (attribute_exists(phy_partition_id) and is_sealed = :notSealed)"),
		UpdateExpression:    aws.String("SET insertion_time = if_not_exists (insertion_time, :now), worker_id = if_not_exists (worker_id, :nullWorker), is_sealed = :notSealed"),
	}
}
