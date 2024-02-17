package dyndao

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"main/worker/domain"
	"reflect"
)

type DynQueryableCollectionDao struct {
	client       *dynamodb.Client
	entityLoader *domain.EntityLoader
}

func NewDynQueryableCollectionDao(client *dynamodb.Client, entityLoader *domain.EntityLoader) *DynQueryableCollectionDao {
	return &DynQueryableCollectionDao{
		client:       client,
		entityLoader: entityLoader,
	}
}

func (dao *DynQueryableCollectionDao) GetItem(collectionId domain.CollectionId, targetType reflect.Type, itemId string, executionContext domain.ExecutionContext) (any, error) {
	response, err := dao.client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String(collectionId.GetTypeName()),
		Key: map[string]types.AttributeValue{
			"collection_id": &types.AttributeValueMemberS{Value: collectionId.Id},
			"item_id":       &types.AttributeValueMemberS{Value: itemId},
		},
		ProjectionExpression: aws.String("current_state"),
	})

	if err != nil {
		return 0, err
	}

	itemStateString := response.Item["current_state"].(*types.AttributeValueMemberS).Value

	return dao.entityLoader.LoadEntity(itemStateString, targetType, executionContext)
}

func (dao *DynQueryableCollectionDao) FindItems(collectionId domain.CollectionId, targetType reflect.Type, attributeName string, attributeValue string, executionContext domain.ExecutionContext) ([]any, error) {
	entitiesState, err := dao.client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String(targetType.Elem().Name()),
		IndexName: aws.String(attributeName),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":collectionId":   &types.AttributeValueMemberS{Value: collectionId.Id},
			":attributeValue": &types.AttributeValueMemberS{Value: attributeValue},
		},
		KeyConditionExpression: aws.String(fmt.Sprintf("collection_id = :collectionId AND %v = :attributeValue", attributeName)),
		Select:                 types.SelectAllAttributes,
	})

	if err != nil {
		return []any{}, err
	}
	if len(entitiesState.Items) == 0 {
		return []any{}, nil
	}

	var entities []any
	for _, item := range entitiesState.Items {
		itemStateString := item["current_state"].(*types.AttributeValueMemberS).Value
		entity, loadErr := dao.entityLoader.LoadEntity(itemStateString, targetType.Elem(), executionContext)
		if loadErr != nil {
			return []any{}, loadErr
		}
		entities = append(entities, entity)
	}
	return entities, nil
}
