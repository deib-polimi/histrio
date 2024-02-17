package dyndao

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/worker/domain"
	"strconv"
	"time"
)

type DynTaskDao struct {
	Client *dynamodb.Client
}

func (dao *DynTaskDao) PullNewActorTasks(workerId string, maxTasksToPull int) ([]domain.ActorTask, error) {
	tasks, err := dao.Client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String("ActorTask"),
		IndexName: aws.String("ActorTaskByWorker"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nullWorker": &types.AttributeValueMemberS{Value: "NULL"},
		},
		KeyConditionExpression: aws.String("worker_id = :nullWorker"),
		Limit:                  aws.Int32(int32(maxTasksToPull)),
		Select:                 types.SelectAllAttributes,
	})

	if err != nil {
		return []domain.ActorTask{}, err
	}

	if len(tasks.Items) == 0 {
		return []domain.ActorTask{}, nil
	}

	var updateItems []dynamodb.UpdateItemInput

	for _, item := range tasks.Items {
		phyPartitionId := item["phy_partition_id"].(*types.AttributeValueMemberS).Value

		updateItems = append(updateItems, dynamodb.UpdateItemInput{
			TableName: aws.String("ActorTask"),
			Key: map[string]types.AttributeValue{
				"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId},
			},
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":workerId":   &types.AttributeValueMemberS{Value: workerId},
				":nullWorker": &types.AttributeValueMemberS{Value: "NULL"},
			},
			ConditionExpression: aws.String("worker_id = :nullWorker"),
			UpdateExpression:    aws.String("SET worker_id = :workerId"),
			ReturnValues:        types.ReturnValueAllNew,
		})
	}

	type pullResult struct {
		isTaskLocked   bool
		phyPartitionId domain.PhysicalPartitionId
	}

	//map-reduce to lock actors
	inputQueue := make(chan *dynamodb.UpdateItemInput, len(updateItems))
	outputQueue := make(chan pullResult, len(updateItems))
	maxConcurrentRequests := 20

	//producer
	go func() {
		for i := range updateItems {
			inputQueue <- &updateItems[i]
		}
		close(inputQueue)
	}()

	//mappers
	for range maxConcurrentRequests {
		go func() {
			for updateItem := range inputQueue {
				item, err := dao.Client.UpdateItem(context.TODO(), updateItem)
				if err != nil {
					outputQueue <- pullResult{isTaskLocked: false}
				} else {
					phyPartitionId, _ := domain.StrToPhyPartitionId(item.Attributes["phy_partition_id"].(*types.AttributeValueMemberS).Value)
					outputQueue <- pullResult{isTaskLocked: true, phyPartitionId: phyPartitionId}
				}
			}
		}()
	}

	var pulledTasks []domain.ActorTask

	//reducer
	for range len(updateItems) {
		result := <-outputQueue
		if result.isTaskLocked {
			pulledTasks = append(pulledTasks, domain.ActorTask{PhyPartitionId: result.phyPartitionId})
		}
	}

	if len(pulledTasks) < len(updateItems) {
		log.Printf("Worker '%v' tried to lock %v tasks, but failed to lock %v of them\n", workerId, len(updateItems), len(updateItems)-len(pulledTasks))
	}

	return pulledTasks, nil
}

func (dao *DynTaskDao) RecoverActorTasks(workerId string) ([]domain.ActorTask, error) {
	tasks, err := dao.Client.Query(context.TODO(), &dynamodb.QueryInput{
		TableName: aws.String("ActorTask"),
		IndexName: aws.String("ActorTaskByWorker"),
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":myWorkerId": &types.AttributeValueMemberS{Value: workerId},
		},
		KeyConditionExpression: aws.String("worker_id = :myWorkerId"),
		Select:                 types.SelectAllAttributes,
	})

	if err != nil {
		return []domain.ActorTask{}, err
	}

	var pulledTasks []domain.ActorTask

	for _, item := range tasks.Items {
		phyPartitionId, _ := domain.StrToPhyPartitionId(item["phy_partition_id"].(*types.AttributeValueMemberS).Value)
		pulledTasks = append(pulledTasks, domain.ActorTask{PhyPartitionId: phyPartitionId})
	}

	return pulledTasks, nil

}

func (dao *DynTaskDao) GetTaskStatus(phyPartitionId domain.PhysicalPartitionId) (domain.TaskStatus, error) {
	task, err := dao.Client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
		ConsistentRead:       aws.Bool(true),
		ProjectionExpression: aws.String("is_sealed"),
	})

	if err != nil {
		return domain.TaskStatus{}, err
	}

	if task.Item == nil {
		return domain.TaskStatus{PhyPartitionId: phyPartitionId, IsActorPassivated: true}, nil
	} else {
		return domain.TaskStatus{PhyPartitionId: phyPartitionId, IsSealed: task.Item["is_sealed"].(*types.AttributeValueMemberBOOL).Value}, nil
	}
}

func (dao *DynTaskDao) AddTask(phyPartitionId domain.PhysicalPartitionId, now time.Time) error {
	_, err := dao.Client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		TableName: aws.String("ActorTask"),
		Key: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":nullWorker": &types.AttributeValueMemberS{Value: "NULL"},
			":notSealed":  &types.AttributeValueMemberBOOL{Value: false},
			":now":        &types.AttributeValueMemberS{Value: strconv.FormatInt(now.UnixMilli(), 10)},
		},
		ConditionExpression: aws.String("attribute_not_exists(phy_partition_id) or (attribute_exists(phy_partition_id) and is_sealed = :notSealed)"),
		UpdateExpression:    aws.String("SET insertion_time = if_not_exists (insertion_time, :now), worker_id = if_not_exists (worker_id, :nullWorker), is_sealed = :notSealed"),
	})

	return err
}
