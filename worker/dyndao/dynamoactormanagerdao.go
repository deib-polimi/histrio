package dyndao

import (
	"context"
	"encoding/json"
	"log"
	"main/worker/domain"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type DynActorManagerDao struct {
	Client   *dynamodb.Client
	workerId string
}

func NewDynActorManagerDao(client *dynamodb.Client, workerId string) *DynActorManagerDao {
	return &DynActorManagerDao{Client: client, workerId: workerId}
}

func (dao *DynActorManagerDao) FetchState(actorId domain.ActorId) (state string, actorType string, err error) {
	response, err := dao.Client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("ActorState"),
		Key: map[string]types.AttributeValue{
			"actor_id": &types.AttributeValueMemberS{Value: actorId.String()},
		},
	})

	if err != nil {
		return "", "", err
	}

	actorState := response.Item["current_state"].(*types.AttributeValueMemberS).Value
	actorTypeStr := response.Item["type"].(*types.AttributeValueMemberS).Value

	return actorState, actorTypeStr, nil
}

func (dao *DynActorManagerDao) ExecuteTransaction(actorId domain.ActorId, newState string, dirtyItems map[domain.CollectionId][]domain.QueryableItem, spawningActors []domain.Actor, consumedMessage domain.ActorMessage, outboxes []domain.Outbox, runId string) error {

	var transactItems []types.TransactWriteItem
	for _, outbox := range outboxes {
		if outbox.DestinationId.PhysicalPartitionName != "-" {
			transactItems = append(transactItems, dao.buildNewInternalMessagesTransactItems(outbox, actorId)...)
		} else {
			transactItems = append(transactItems, dao.buildNewExternalMessagesTransactItems(outbox, actorId, runId)...)
		}
	}

	transactItems = append(transactItems, types.TransactWriteItem{Delete: &types.Delete{
		TableName: aws.String("ActorInbox"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: actorId.PhyPartitionId.String()},
			"timestamp":        &types.AttributeValueMemberS{Value: consumedMessage.Id.UniqueTimestamp},
		},
	}})

	transactItems = append(transactItems, types.TransactWriteItem{Update: &types.Update{
		TableName: aws.String("ActorState"),
		Key: map[string]types.AttributeValue{
			"actor_id": &types.AttributeValueMemberS{Value: actorId.String()},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{":state": &types.AttributeValueMemberS{Value: newState}},
		UpdateExpression:          aws.String("SET current_state = :state"),
	}})

	//spawn new actors
	for _, spawningActor := range spawningActors {
		typeName := reflect.TypeOf(spawningActor).Elem().Name()
		initialStateJson, err := json.Marshal(spawningActor)
		if err != nil {
			return err
		}
		initialState := string(initialStateJson)

		transactItems = append(transactItems, types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String("ActorState"),
			Item: map[string]types.AttributeValue{
				"actor_id":      &types.AttributeValueMemberS{Value: spawningActor.GetId().String()},
				"current_state": &types.AttributeValueMemberS{Value: initialState},
				"type":          &types.AttributeValueMemberS{Value: typeName},
			},
		}})
	}

	//append collection items changes
	for collectionId, items := range dirtyItems {
		for _, item := range items {
			itemState, err := json.Marshal(item)
			if err != nil {
				return err
			}

			expressions := make(map[string]types.AttributeValue)
			expressions[":state"] = &types.AttributeValueMemberS{Value: string(itemState)}
			for attrName, attrValue := range item.GetQueryableAttributes() {
				expressions[":"+attrName] = &types.AttributeValueMemberS{Value: attrValue}
			}

			updateExpression := "SET current_state = :state"
			sep := ", "
			for attributeName := range item.GetQueryableAttributes() {
				updateExpression += sep
				updateExpression += attributeName + " = :" + attributeName
			}

			transactItems = append(transactItems, types.TransactWriteItem{Update: &types.Update{
				TableName: aws.String(collectionId.GetTypeName()),
				Key: map[string]types.AttributeValue{
					"collection_id": &types.AttributeValueMemberS{Value: collectionId.Id},
					"item_id":       &types.AttributeValueMemberS{Value: item.GetId()},
				},
				ExpressionAttributeValues: expressions,
				UpdateExpression:          aws.String(updateExpression),
			}})
		}
	}

	_, err := dao.Client.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems: transactItems,
	})
	return err

}

func (dao *DynActorManagerDao) buildNewInternalMessagesTransactItems(outbox domain.Outbox, actorId domain.ActorId) []types.TransactWriteItem {
	var transactItems []types.TransactWriteItem

	var offset int64 = 0
	destinationId := outbox.DestinationId
	currentTimestamp := time.Now().UnixMilli()

	for _, message := range outbox.Messages {
		messageId := strconv.FormatInt(currentTimestamp+offset, 10) + "#" + dao.workerId + actorId.InstanceId
		messageJson, err := json.Marshal(message.Payload)

		if err != nil {
			log.Fatalf("Could not serialize message with id %v", messageId)
		}

		messageType := reflect.TypeOf(message.Payload)

		transactItems = append(transactItems, types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String("ActorInbox"),
			Item: map[string]types.AttributeValue{
				"phy_partition_id": &types.AttributeValueMemberS{Value: destinationId.String()},
				"timestamp":        &types.AttributeValueMemberS{Value: messageId},
				"receiver_id":      &types.AttributeValueMemberS{Value: message.ActorId.String()},
				"sender_id":        &types.AttributeValueMemberS{Value: actorId.String()},
				"content":          &types.AttributeValueMemberS{Value: string(messageJson)},
				"type":             &types.AttributeValueMemberS{Value: messageType.Name()},
			},
		}})

		offset++
	}

	return transactItems
}

func (dao *DynActorManagerDao) buildNewExternalMessagesTransactItems(outbox domain.Outbox, sender domain.ActorId, runId string) []types.TransactWriteItem {
	var transactItems []types.TransactWriteItem
	var offset int64 = 0

	for _, message := range outbox.Messages {
		messageJson, err := json.Marshal(message.Payload)

		if err != nil {
			log.Fatalf("Could not serialize a message: %v", err)
		}

		messageType := reflect.TypeOf(message.Payload)

		transactItems = append(transactItems, types.TransactWriteItem{Put: &types.Put{
			TableName: aws.String("Outbox"),
			Item: map[string]types.AttributeValue{
				"run_id":         &types.AttributeValueMemberS{Value: runId},
				"correlation_id": &types.AttributeValueMemberS{Value: message.ActorId.InstanceId},
				"timestamp":      &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10)},
				"sender_id":      &types.AttributeValueMemberS{Value: sender.String()},
				"content":        &types.AttributeValueMemberS{Value: string(messageJson)},
				"type":           &types.AttributeValueMemberS{Value: messageType.Name()},
			},
		}})

		offset++
	}

	return transactItems
}
