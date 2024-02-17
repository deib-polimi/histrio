package dynamoutils

import (
	"context"
	"encoding/json"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	"github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"log"
	"main/baseline/hotel-reservation/model"
	"main/utils"
	"main/worker/domain"
	"math/rand"
	net "net/http"
	"os"
	"reflect"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type TableDefinition struct {
	TableName string

	PartitionKey         AttributeDefinition
	SortKey              AttributeDefinition
	AdditionalAttributes []AttributeDefinition

	SecondaryIndexes []SecondaryIndexDefinition
}

type SecondaryIndexDefinition struct {
	IndexName string

	PartitionKeyName string
	SortKeyName      string
}

type AttributeDefinition struct {
	Name       string
	ScalarType types.ScalarAttributeType
}

func CreateTable(client *dynamodb.Client, tableDefinition TableDefinition) (*types.TableDescription, error) {
	var tableDesc *types.TableDescription
	attributeDefinitions := []types.AttributeDefinition{{
		AttributeName: aws.String(tableDefinition.PartitionKey.Name),
		AttributeType: tableDefinition.PartitionKey.ScalarType,
	}}
	if tableDefinition.SortKey.Name != "" {
		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(tableDefinition.SortKey.Name),
			AttributeType: tableDefinition.SortKey.ScalarType,
		})
	}

	for _, additionalAttribute := range tableDefinition.AdditionalAttributes {
		attributeDefinitions = append(attributeDefinitions, types.AttributeDefinition{
			AttributeName: aws.String(additionalAttribute.Name),
			AttributeType: additionalAttribute.ScalarType,
		})
	}

	tableSchema := createKeySchema(
		tableDefinition.PartitionKey.Name,
		tableDefinition.SortKey.Name,
	)

	globalSecondaryIndexes := []types.GlobalSecondaryIndex{}
	for _, index := range tableDefinition.SecondaryIndexes {
		indexSchema := createKeySchema(
			index.PartitionKeyName,
			index.SortKeyName,
		)

		globalSecondaryIndexes = append(globalSecondaryIndexes, types.GlobalSecondaryIndex{
			IndexName:  aws.String(index.IndexName),
			KeySchema:  indexSchema,
			Projection: &types.Projection{ProjectionType: types.ProjectionTypeAll},
		})
	}

	if len(globalSecondaryIndexes) == 0 {
		globalSecondaryIndexes = nil
	}

	createTableInput := dynamodb.CreateTableInput{
		TableName:              aws.String(tableDefinition.TableName),
		AttributeDefinitions:   attributeDefinitions,
		KeySchema:              tableSchema,
		BillingMode:            types.BillingModePayPerRequest,
		GlobalSecondaryIndexes: globalSecondaryIndexes,
	}

	table, err := client.CreateTable(context.TODO(), &createTableInput)

	if err != nil {
		log.Printf("Couldn't create table %v. Here's why: %v\n", tableDefinition.TableName, err)
	} else {
		waiter := dynamodb.NewTableExistsWaiter(client)
		err = waiter.Wait(context.TODO(), &dynamodb.DescribeTableInput{
			TableName: aws.String(tableDefinition.TableName)}, 5*time.Minute)
		if err != nil {
			log.Printf("Wait for table exists failed. Here's why: %v\n", err)
		}
		tableDesc = table.TableDescription
	}
	return tableDesc, err

}

func CreateLocalClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		// CHANGE THIS TO proper region TO USE AWS proper
		config.WithRegion("localhost"),
		// Comment the below out if not using localhost
		config.WithEndpointResolver(aws.EndpointResolverFunc(
			func(service, region string) (aws.Endpoint, error) {
				return aws.Endpoint{URL: "http://localhost:8000", SigningRegion: "localhost"}, nil
			})),
		config.WithHTTPClient(
			http.NewBuildableClient().
				WithTransportOptions(func(tr *net.Transport) {
					tr.ExpectContinueTimeout = 0
					tr.MaxIdleConns = 1000
				}),
		),
		config.WithClientLogMode(aws.LogRetries),
	)

	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.Credentials = credentials.NewStaticCredentialsProvider("b59xng", "b2sc6o", "")
	})

	return client
}

func CreateAwsClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-west-3"),
		config.WithClientLogMode(aws.LogRetries),
		config.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(so *retry.StandardOptions) {
				so.RateLimiter = ratelimit.NewTokenRateLimit(1000000)
				so.MaxAttempts = 0
			})
		}),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg)
	return client
}

func CreateAwsPrivateClient() *dynamodb.Client {
	cfg, err := config.LoadDefaultConfig(context.TODO(),
		config.WithRegion("eu-west-3"),
		config.WithClientLogMode(aws.LogRetries),
	)
	if err != nil {
		log.Fatalf("unable to load SDK config, %v", err)
	}

	client := dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) {
		o.BaseEndpoint = aws.String(os.Getenv("DDB_URL"))
	})
	return client
}

func GetExistingTableNames(client *dynamodb.Client) (tableNames []string, err error) {
	result, err := client.ListTables(context.TODO(), &dynamodb.ListTablesInput{})
	if err != nil {
		return []string{}, err
	}
	return result.TableNames, nil
}

func DeleteTable(client *dynamodb.Client, tableName string) (*dynamodb.DeleteTableOutput, error) {
	table, err := client.DeleteTable(context.TODO(), &dynamodb.DeleteTableInput{TableName: &tableName})

	if err != nil {
		log.Printf("Could not delete table %v: %v\n", tableName, err)
	}

	return table, err
}

func CreateActorStateTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "ActorState",
		PartitionKey: AttributeDefinition{"actor_id", types.ScalarAttributeTypeS},
	}

	return CreateTable(client, tableDefinition)
}

func CreateActorInboxTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "ActorInbox",
		PartitionKey: AttributeDefinition{"phy_partition_id", types.ScalarAttributeTypeS},
		SortKey:      AttributeDefinition{"timestamp", types.ScalarAttributeTypeS},
	}

	return CreateTable(client, tableDefinition)
}

func CreateActorTaskTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "ActorTask",
		PartitionKey: AttributeDefinition{"phy_partition_id", types.ScalarAttributeTypeS},
		AdditionalAttributes: []AttributeDefinition{
			{
				Name:       "worker_id",
				ScalarType: types.ScalarAttributeTypeS,
			},
			{
				Name:       "insertion_time",
				ScalarType: types.ScalarAttributeTypeS,
			},
		},
		SecondaryIndexes: []SecondaryIndexDefinition{
			{
				IndexName:        "ActorTaskByWorker",
				PartitionKeyName: "worker_id",
				SortKeyName:      "insertion_time",
			},
		},
	}

	return CreateTable(client, tableDefinition)
}

func CreateEntityTable(client *dynamodb.Client, entityTypeName string, entityExample domain.QueryableItem) (*types.TableDescription, error) {
	var queryableAttributes []AttributeDefinition
	var secondaryIndexes []SecondaryIndexDefinition
	for attributeName, _ := range entityExample.GetQueryableAttributes() {
		queryableAttributes = append(queryableAttributes, AttributeDefinition{
			Name:       attributeName,
			ScalarType: types.ScalarAttributeTypeS,
		})
		secondaryIndexes = append(secondaryIndexes, SecondaryIndexDefinition{
			IndexName:        attributeName,
			PartitionKeyName: "collection_id",
			SortKeyName:      attributeName,
		})
	}

	tableDefinition := TableDefinition{
		TableName:            entityTypeName,
		PartitionKey:         AttributeDefinition{"collection_id", types.ScalarAttributeTypeS},
		SortKey:              AttributeDefinition{"item_id", types.ScalarAttributeTypeS},
		AdditionalAttributes: queryableAttributes,
		SecondaryIndexes:     secondaryIndexes,
	}

	return CreateTable(client, tableDefinition)
}

func CreatePartitionTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "Partitions",
		PartitionKey: AttributeDefinition{"partition_name", types.ScalarAttributeTypeS},
		SortKey:      AttributeDefinition{"shard_id", types.ScalarAttributeTypeS},
		AdditionalAttributes: []AttributeDefinition{
			{
				Name:       "allocated_actors_count",
				ScalarType: types.ScalarAttributeTypeN,
			},
		},
		SecondaryIndexes: []SecondaryIndexDefinition{
			{
				IndexName:        "ShardsOrderedByActorsCount",
				PartitionKeyName: "partition_name",
				SortKeyName:      "allocated_actors_count",
			},
		},
	}

	return CreateTable(client, tableDefinition)

}

func CreateOutboxTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "Outbox",
		PartitionKey: AttributeDefinition{"run_id", types.ScalarAttributeTypeS},
		SortKey:      AttributeDefinition{"correlation_id", types.ScalarAttributeTypeS},
	}

	return CreateTable(client, tableDefinition)
}

func CreateBaselineTable(client *dynamodb.Client) (*types.TableDescription, error) {
	tableDefinition := TableDefinition{
		TableName:    "BaselineTable",
		PartitionKey: AttributeDefinition{"PK", types.ScalarAttributeTypeS},
		SortKey:      AttributeDefinition{"SK", types.ScalarAttributeTypeS},
	}

	return CreateTable(client, tableDefinition)
}

func AddActorState(client *dynamodb.Client, actorId domain.ActorId, actor any) error {

	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("ActorState"),
		Item:      buildActorStatePutItem(actorId, actor),
	})

	return err
}

func AddActorStateBatch(client *dynamodb.Client, actors map[domain.ActorId]any) error {
	var putItems []map[string]types.AttributeValue

	for actorId, actor := range actors {
		putItems = append(putItems, buildActorStatePutItem(actorId, actor))
	}

	return doPaginatedBatchWrite(client, "ActorState", putItems)

}

func buildActorStatePutItem(actorId domain.ActorId, actor any) map[string]types.AttributeValue {
	state, err := json.Marshal(actor)

	if err != nil {
		log.Fatal(err)
	}

	return map[string]types.AttributeValue{
		"actor_id":      &types.AttributeValueMemberS{Value: actorId.String()},
		"current_state": &types.AttributeValueMemberS{Value: string(state)},
		"type":          &types.AttributeValueMemberS{Value: reflect.TypeOf(actor).Elem().Name()},
	}
}

func doPaginatedBatchWrite(client *dynamodb.Client, tableName string, items []map[string]types.AttributeValue) error {
	maxBatchSize := 1 // forced by aws

	parallelJobExecutor := utils.NewSimpleParallelJobExecutor(getConcurrentLoadingUnits())
	parallelJobExecutor.RegisterConsumer(func(tag string) bool { return true }, NewSimpleBatchRequestConsumer(len(items)/25))
	parallelJobExecutor.RegisterErrorHandler(func(err error) { log.Printf("Encountered error while loading batch: %v\n", err) })
	parallelJobExecutor.Start()

	var writeRequests []types.WriteRequest
	for _, item := range items {
		writeRequests = append(writeRequests, types.WriteRequest{PutRequest: &types.PutRequest{Item: item}})
	}

	startIndex := 0
	for {
		batchSize := min(len(writeRequests)-startIndex, maxBatchSize)
		if batchSize == 0 {
			break
		}

		excludedEndIndex := startIndex + batchSize

		func(startIndex int, excludedEndIndex int) {
			parallelJobExecutor.SubmitJob(func() (utils.Result, error) {
				_, err := client.BatchWriteItem(context.TODO(), &dynamodb.BatchWriteItemInput{
					RequestItems: map[string][]types.WriteRequest{
						tableName: writeRequests[startIndex:excludedEndIndex],
					},
				})
				time.Sleep(10 * time.Millisecond)
				return utils.Result{}, err
			})

		}(startIndex, excludedEndIndex)

		startIndex = excludedEndIndex
	}

	parallelJobExecutor.Stop()

	return nil
}

type SimpleBatchRequestConsumer struct {
	totalBatchesCount    int
	consumedBatchesCount int
}

func NewSimpleBatchRequestConsumer(totalBatchesCount int) *SimpleBatchRequestConsumer {
	return &SimpleBatchRequestConsumer{totalBatchesCount: totalBatchesCount}
}

func (c *SimpleBatchRequestConsumer) Consume(_ utils.Result) {
	c.consumedBatchesCount++
	if c.consumedBatchesCount%5 == 0 {
		log.Printf("Consumed %v out of %v batches", c.consumedBatchesCount, c.totalBatchesCount)
	}
}

func mapPaginatedItems(client *dynamodb.Client, tableName string, itemMapper func(item map[string]types.AttributeValue) map[string]types.AttributeValue) error {
	var lastKey map[string]types.AttributeValue

	parallelJobExecutor := utils.NewSimpleParallelJobExecutor(getConcurrentLoadingUnits())
	parallelJobExecutor.RegisterErrorHandler(func(err error) { log.Printf("Encountered error while setting actor task: %v\n", err) })
	parallelJobExecutor.Start()

	for {
		result, err := client.Scan(context.TODO(), &dynamodb.ScanInput{
			TableName:         aws.String(tableName),
			ExclusiveStartKey: lastKey,
		})

		if err != nil {
			log.Fatal(err)
		}
		lastKey = result.LastEvaluatedKey

		for _, item := range result.Items {
			mappedItem := itemMapper(item)

			parallelJobExecutor.SubmitJob(func() (utils.Result, error) {
				_, putErr := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
					TableName: aws.String(tableName),
					Item:      mappedItem,
				})
				time.Sleep(10 * time.Millisecond)
				return utils.Result{}, putErr
			})
		}

		if len(result.LastEvaluatedKey) == 0 {
			break
		}
	}

	parallelJobExecutor.Stop()

	return nil
}

func AddMessage(
	client *dynamodb.Client,
	message domain.ActorMessage,
	destination domain.ActorId) error {

	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("ActorInbox"),
		Item:      buildMessagePutItem(message, destination, 0),
	})

	return err

}

func AddMessageBatch(client *dynamodb.Client, messageAndDestination []utils.Pair[domain.ActorMessage, domain.ActorId]) error {
	var putItems []map[string]types.AttributeValue
	seed := 0
	for _, msgAndDst := range messageAndDestination {
		putItems = append(putItems, buildMessagePutItem(msgAndDst.First, msgAndDst.Second, seed))
		seed++
	}

	err := doPaginatedBatchWrite(client, "ActorInbox", putItems)

	return err
}

func buildMessagePutItem(message domain.ActorMessage, destination domain.ActorId, seed int) map[string]types.AttributeValue {
	messageJson, serializationErr := json.Marshal(message.Content)
	messageType := reflect.TypeOf(message.Content)

	if serializationErr != nil {
		log.Fatal(serializationErr)
	}
	return map[string]types.AttributeValue{
		"phy_partition_id": &types.AttributeValueMemberS{Value: destination.PhyPartitionId.String()},
		"timestamp":        &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10) + "#" + strconv.Itoa(seed) + "#" + strconv.Itoa(rand.Int()%1000)},
		"receiver_id":      &types.AttributeValueMemberS{Value: destination.String()},
		"sender_id":        &types.AttributeValueMemberS{Value: message.SenderId.String()},
		"content":          &types.AttributeValueMemberS{Value: string(messageJson)},
		"type":             &types.AttributeValueMemberS{Value: messageType.Name()},
	}
}

func AddActorTask(
	client *dynamodb.Client,
	phyPartitionId domain.PhysicalPartitionId,
	sealed bool,
	workerId string,
) error {
	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String("ActorTask"),
		Item: map[string]types.AttributeValue{
			"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
			"insertion_time":   &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10) + "#" + strconv.Itoa(rand.Int()%100)},
			"worker_id":        &types.AttributeValueMemberS{Value: workerId},
			"is_sealed":        &types.AttributeValueMemberBOOL{Value: sealed},
		},
	})

	return err
}

func AddActorTaskBatch(client *dynamodb.Client, phyPartitionIds []domain.PhysicalPartitionId) error {
	var putItems []map[string]types.AttributeValue
	for _, phyPartitionId := range phyPartitionIds {
		putItems = append(putItems, buildActorTaskPutItem(phyPartitionId))
	}

	return doPaginatedBatchWrite(client, "ActorTask", putItems)
}

func EquallyAssignTasksToWorkers(client *dynamodb.Client, numberOfWorkers int) error {
	return mapPaginatedItems(client, "ActorTask",
		func(actorTaskItem map[string]types.AttributeValue) map[string]types.AttributeValue {
			pickedWorkerIndex := rand.Intn(numberOfWorkers)
			workerId := "Worker-" + strconv.Itoa(pickedWorkerIndex)
			actorTaskItem["worker_id"] = &types.AttributeValueMemberS{Value: workerId}
			return actorTaskItem
		},
	)
}

func buildActorTaskPutItem(phyPartitionId domain.PhysicalPartitionId) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"phy_partition_id": &types.AttributeValueMemberS{Value: phyPartitionId.String()},
		"insertion_time":   &types.AttributeValueMemberS{Value: strconv.FormatInt(time.Now().UnixMilli(), 10) + "#" + strconv.Itoa(rand.Int()%100)},
		"worker_id":        &types.AttributeValueMemberS{Value: "NULL"},
		"is_sealed":        &types.AttributeValueMemberBOOL{Value: false},
	}
}

func AddEntity(client *dynamodb.Client, collectionId domain.CollectionId, item domain.QueryableItem) error {
	_, err := client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(collectionId.GetTypeName()),
		Item:      buildEntityPutItem(collectionId, item),
	})

	return err
}

func buildEntityPutItem(collectionId domain.CollectionId, item domain.QueryableItem) map[string]types.AttributeValue {
	baseItem := map[string]types.AttributeValue{
		"collection_id": &types.AttributeValueMemberS{Value: collectionId.Id},
		"item_id":       &types.AttributeValueMemberS{Value: item.GetId()},
	}
	newState, err := json.Marshal(item)
	if err != nil {
		log.Fatal(err)
	}

	baseItem["current_state"] = &types.AttributeValueMemberS{Value: string(newState)}
	for attributeName, attributeValue := range item.GetQueryableAttributes() {
		baseItem[attributeName] = &types.AttributeValueMemberS{Value: attributeValue}
	}

	return baseItem
}

func AddEntityBatch(client *dynamodb.Client, collectionIdAndItem []utils.Pair[domain.CollectionId, domain.QueryableItem]) error {
	var putItems []map[string]types.AttributeValue
	for _, pair := range collectionIdAndItem {
		putItems = append(putItems, buildEntityPutItem(pair.First, pair.Second))
	}

	putItemsByTable := make(map[string][]map[string]types.AttributeValue)

	for _, pair := range collectionIdAndItem {
		putItemsByTable[pair.First.GetTypeName()] = append(putItemsByTable[pair.First.GetTypeName()], buildEntityPutItem(pair.First, pair.Second))
	}

	for tableName, perTablePutItems := range putItemsByTable {
		err := doPaginatedBatchWrite(client, tableName, perTablePutItems)
		if err != nil {
			return err
		}
	}

	return nil
}

// Baseline utils

func AddBaselineHotelsBatch(client *dynamodb.Client, hotels []BaselineHotel) error {
	var putItems []map[string]types.AttributeValue
	for _, hotel := range hotels {
		hotelPutItem := map[string]types.AttributeValue{
			"PK":                        &types.AttributeValueMemberS{Value: hotel.Id},
			"SK":                        &types.AttributeValueMemberS{Value: "Info"},
			"hotel_failed_reservations": &types.AttributeValueMemberN{Value: "0"},
			"hotel_reservations":        &types.AttributeValueMemberN{Value: "0"},
		}
		putItems = append(putItems, hotelPutItem)

		for _, weekAvailability := range hotel.WeeksAvailabilities {
			jsonWeekAvailability, err := json.Marshal(weekAvailability)
			if err != nil {
				return err
			}
			putItem := map[string]types.AttributeValue{
				"PK":                 &types.AttributeValueMemberS{Value: hotel.Id},
				"SK":                 &types.AttributeValueMemberS{Value: "WeekAvailability#" + weekAvailability.WeekId},
				"locked_instance_id": &types.AttributeValueMemberS{Value: "NULL"},
				"current_state":      &types.AttributeValueMemberS{Value: string(jsonWeekAvailability)},
			}
			putItems = append(putItems, putItem)
		}
	}

	return doPaginatedBatchWrite(client, "BaselineTable", putItems)
}

func AddBaselineHotelUsersBatch(client *dynamodb.Client, userIds []string) error {
	var putItems []map[string]types.AttributeValue

	for _, userId := range userIds {
		putItem := map[string]types.AttributeValue{
			"PK":                        &types.AttributeValueMemberS{Value: userId},
			"SK":                        &types.AttributeValueMemberS{Value: "Info"},
			"total_reservations":        &types.AttributeValueMemberN{Value: "0"},
			"total_failed_reservations": &types.AttributeValueMemberN{Value: "0"},
		}
		putItems = append(putItems, putItem)
	}

	return doPaginatedBatchWrite(client, "BaselineTable", putItems)

}

type BaselineHotel struct {
	Id                  string
	WeeksAvailabilities []model.WeekAvailability
}

func AddBaselineAccountsBatch(client *dynamodb.Client, accountsCount int) error {
	var putItems []map[string]types.AttributeValue

	for i := range accountsCount {
		putItems = append(putItems, map[string]types.AttributeValue{
			"PK":                 &types.AttributeValueMemberS{Value: "Account/" + strconv.Itoa(i)},
			"SK":                 &types.AttributeValueMemberS{Value: "Info"},
			"locked_instance_id": &types.AttributeValueMemberS{Value: "NULL"},
			"amount":             &types.AttributeValueMemberN{Value: "10000"},
		})
	}

	return doPaginatedBatchWrite(client, "BaselineTable", putItems)

}

func createKeySchema(
	partitionKeyName string, sortKeyName string) []types.KeySchemaElement {
	schema := []types.KeySchemaElement{{
		AttributeName: aws.String(partitionKeyName),
		KeyType:       types.KeyTypeHash,
	}}

	if sortKeyName != "" {
		schema = append(schema, types.KeySchemaElement{
			AttributeName: aws.String(sortKeyName),
			KeyType:       types.KeyTypeRange,
		})
	}

	return schema
}

func getConcurrentLoadingUnits() int {
	concurrentLoadingUnits := 10
	concurrentLoadingUnitsEnv := os.Getenv("cd ")
	if concurrentLoadingUnitsEnv != "" {
		var err error
		concurrentLoadingUnits, err = strconv.Atoi(concurrentLoadingUnitsEnv)
		if err != nil {
			log.Fatalf("Malformed CONCURRENT_LOADING_UNITS env variable (%v): %v", concurrentLoadingUnitsEnv, err)
		}
	}
	return concurrentLoadingUnits
}
