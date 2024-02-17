package dyndao

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/worker/domain"
	"reflect"
	"strconv"
)

type DynActorSpawningDao struct {
	client *dynamodb.Client
}

func NewDynActorSpawningDao(client *dynamodb.Client) *DynActorSpawningDao {
	return &DynActorSpawningDao{
		client: client,
	}
}

func (dao *DynActorSpawningDao) FetchPartition(partitionName domain.PartitionName) (*domain.FreshPartition, error) {
	shardsResponse, err := dao.client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String("Partitions"),
		IndexName: aws.String("ShardsOrderedByActorsCount"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partitionName": &types.AttributeValueMemberS{Value: string(partitionName)},
		},
		KeyConditionExpression: aws.String("partition_name = :partitionName"),
		Limit:                  aws.Int32(10),
		Select:                 types.SelectAllAttributes,
	})

	if err != nil {
		return nil, err
	}

	if len(shardsResponse.Items) == 0 {
		return nil, errors.New("cannot find partition named '" + string(partitionName) + "'")
	}

	var shards []*domain.Shard
	for _, shardItem := range shardsResponse.Items {
		shardId := domain.ShardId(shardItem["shard_id"].(*types.AttributeValueMemberS).Value)
		actorsCount, err := strconv.Atoi(shardItem["allocated_actors_count"].(*types.AttributeValueMemberN).Value)
		if err != nil {
			log.Printf("ERROR: Cannot parse the allocated actors count for shard %v\n", shardId)
		}
		shards = append(shards, domain.NewShard(shardId, actorsCount))
	}

	return domain.NewFreshPartition(partitionName, shards), nil
}

func (dao *DynActorSpawningDao) IncrementActorsCount(partitionName domain.PartitionName, shardId domain.ShardId) error {
	_, err := dao.client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("Partitions"),
		Key: map[string]types.AttributeValue{
			"partition_name": &types.AttributeValueMemberS{Value: string(partitionName)},
			"shard_id":       &types.AttributeValueMemberS{Value: string(shardId)},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":deltaActorsCount": &types.AttributeValueMemberN{Value: "1"},
		},
		UpdateExpression: aws.String("ADD allocated_actors_count :deltaActorsCount"),
		ReturnValues:     types.ReturnValueAllNew,
	})

	return err
}

func (dao *DynActorSpawningDao) AddShards(partitionName domain.PartitionName, minNewShardsCount int) ([]*domain.Shard, error) {
	shardsResponse, err := dao.client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String("Partitions"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":partitionName": &types.AttributeValueMemberS{Value: string(partitionName)},
		},
		KeyConditionExpression: aws.String("partition_name = :partitionName"),
		Limit:                  aws.Int32(1),
		Select:                 types.SelectAllAttributes,
		ScanIndexForward:       aws.Bool(false),
	})

	if err != nil {
		return nil, err
	}

	if len(shardsResponse.Items) == 0 {
		return nil, errors.New("cannot find partition named '" + string(partitionName) + "'")
	}

	highestShardItem := shardsResponse.Items[0]
	highestShardId, err := strconv.Atoi(highestShardItem["shard_id"].(*types.AttributeValueMemberS).Value)

	if err != nil {
		return nil, err
	}

	type pullResult struct {
		isShardCreated bool
		shardId        domain.ShardId
	}

	//map-reduce to create new shards
	inputQueue := make(chan *dynamodb.UpdateItemInput, minNewShardsCount)
	outputQueue := make(chan pullResult, minNewShardsCount)

	//producer
	go func() {
		for i := range minNewShardsCount {
			inputQueue <- &dynamodb.UpdateItemInput{
				TableName: aws.String("Partitions"),
				Key: map[string]types.AttributeValue{
					"partition_name": &types.AttributeValueMemberS{Value: string(partitionName)},
					"shard_id":       &types.AttributeValueMemberS{Value: strconv.Itoa(i + highestShardId + 1)},
				},
				ExpressionAttributeValues: map[string]types.AttributeValue{
					":allocatedActorsCount": &types.AttributeValueMemberN{Value: "0"},
				},
				UpdateExpression: aws.String("SET allocated_actors_count = if_not_exists (allocated_actors_count, :allocatedActorsCount)"),
				ReturnValues:     types.ReturnValueAllNew,
			}
		}
		close(inputQueue)
	}()

	//mappers
	for range minNewShardsCount {
		go func() {
			for updateItem := range inputQueue {
				item, updateErr := dao.client.UpdateItem(context.TODO(), updateItem)
				if updateErr != nil {
					outputQueue <- pullResult{isShardCreated: false}
				} else {
					outputQueue <- pullResult{isShardCreated: true, shardId: domain.ShardId(item.Attributes["shard_id"].(*types.AttributeValueMemberS).Value)}
				}
			}
		}()
	}

	var newShards []*domain.Shard

	//reducer
	for range minNewShardsCount {
		result := <-outputQueue
		if result.isShardCreated {
			newShards = append(newShards, domain.NewShard(result.shardId, 0))
		}
	}

	return newShards, nil
}

func (dao *DynActorSpawningDao) AddPartition(partitionName string) error {
	updateItem := &dynamodb.UpdateItemInput{
		TableName: aws.String("Partitions"),
		Key: map[string]types.AttributeValue{
			"partition_name": &types.AttributeValueMemberS{Value: partitionName},
			"shard_id":       &types.AttributeValueMemberS{Value: "0"},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":allocatedActorsCount": &types.AttributeValueMemberN{Value: "0"},
		},
		UpdateExpression: aws.String("SET allocated_actors_count = if_not_exists (allocated_actors_count, :allocatedActorsCount)"),
	}

	_, err := dao.client.UpdateItem(context.TODO(), updateItem)

	return err
}

func (dao *DynActorSpawningDao) StoreActor(actor domain.Actor) error {
	actorState, err := json.Marshal(actor)
	if err != nil {
		return err
	}
	_, err1 := dao.client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("ActorState"),
		Item: map[string]types.AttributeValue{
			"actor_id":      &types.AttributeValueMemberS{Value: actor.GetId().String()},
			"current_state": &types.AttributeValueMemberS{Value: string(actorState)},
			"type":          &types.AttributeValueMemberS{Value: reflect.TypeOf(actor).Elem().Name()},
		},
	})

	return err1
}
