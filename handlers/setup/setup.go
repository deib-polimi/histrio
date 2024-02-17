package main

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
	"main/worker/domain"
	"slices"
)

var client *dynamodb.Client

func init() {
	client = dynamoutils.CreateAwsClient()
}

func handler(_ context.Context, evt json.RawMessage) error {
	//var parameters *benchmark.HotelReservationParameters
	//err := json.Unmarshal(evt, parameters)
	//if err != nil {
	//	return err
	//}

	existingTableNames, err := dynamoutils.GetExistingTableNames(client)

	if err != nil {
		return err
	}

	if !slices.Contains(existingTableNames, "ActorState") {
		_, err = dynamoutils.CreateActorStateTable(client)
		if err != nil {
			return err
		}
	}

	if !slices.Contains(existingTableNames, "ActorInbox") {
		_, err = dynamoutils.CreateActorInboxTable(client)
		if err != nil {
			return err
		}
	}

	if !slices.Contains(existingTableNames, "ActorTask") {
		_, err = dynamoutils.CreateActorTaskTable(client)
		if err != nil {
			return err
		}
	}

	if !slices.Contains(existingTableNames, "WeekAvailability") {
		_, err = dynamoutils.CreateEntityTable(client, "WeekAvailability", &domain.WeekAvailability{})
		if err != nil {
			return err
		}
	}

	if !slices.Contains(existingTableNames, "Partitions") {
		_, err = dynamoutils.CreatePartitionTable(client)
		if err != nil {
			return err
		}
	}

	if !slices.Contains(existingTableNames, "Outbox") {
		_, err = dynamoutils.CreateOutboxTable(client)
		if err != nil {
			return err
		}
	}

	return nil
}

func main() {
	lambda.Start(handler)
}
