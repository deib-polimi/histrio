package domain

import (
	"errors"
	"main/utils"
	"reflect"
	"strings"
	"time"
)

type ActorTask struct {
	PhyPartitionId PhysicalPartitionId
}

type TaskDao interface {
	PullNewActorTasks(workerId string, maxTasksToPull int) ([]ActorTask, error)
	RecoverActorTasks(workerId string) ([]ActorTask, error)
	GetTaskStatus(phyPartitionId PhysicalPartitionId) (TaskStatus, error)
	AddTask(phyPartitionId PhysicalPartitionId, now time.Time) error
}

type TaskStatus struct {
	PhyPartitionId    PhysicalPartitionId
	IsSealed          bool
	IsActorPassivated bool
}

type ActorManagerDao interface {
	FetchState(actorId ActorId) (state string, actorType string, err error)
	ExecuteTransaction(actorId ActorId, newState string, dirtyItems map[CollectionId][]QueryableItem, spawningActors []Actor, consumedMessage ActorMessage, outboxes []Outbox, runId string) error
}

type PhyPartitionManagerDao interface {
	FetchNewMessagesFromInbox(phyPartitionId PhysicalPartitionId) (inbox []ActorMessage, err error)
	SealPhysicalPartition(phyPartitionId PhysicalPartitionId) error
	UnsealPhysicalPartition(phyPartitionId PhysicalPartitionId) error
	DeleteActorTask(phyPartitionId PhysicalPartitionId) error
	ForgetMessage(identifier MessageIdentifier)
	ReleasePhyPartition(phyPartitionId PhysicalPartitionId, workerId string) error
}

type ActorManager interface {
	IsActorLoaded() bool
	IsQueueEmpty() bool
	GetPhyPartitionId() PhysicalPartitionId
	GetActorId() ActorId
	ReplenishQueue(*utils.Queue[ActorMessage])
	GetQueueSize() int
	PrepareMessageProcessing() (RecipientsIds, error)
	CommitMessageProcessing() error
	ForceMessageProcessingRollback()
	AddItemCollector(collector ItemCollector)
	AddSpawningActorsCollector(collector SpawningActorsCollector)
	AddMessageCollector(collector MessageCollector)
	AddBenchmarkHelper(helper *BenchmarkHelper)
}

type PhysicalPartitionManager interface {
	GetId() PhysicalPartitionId
	GetActiveActorsCount() int
	GetInboxesSize() int
	FetchInboxes() (int, error)
	PopReadyActorManagers() []ActorManager
	AcceptCompletedActorManager(ActorManager)
	TryPassivate() bool
	Release(workerId string) error
}

type PhysicalPartitionId struct {
	PartitionName         string
	PhysicalPartitionName string
}

func (p PhysicalPartitionId) String() string {
	return p.PartitionName + "/" + p.PhysicalPartitionName
}

func StrToPhyPartitionId(id string) (PhysicalPartitionId, error) {
	parts := strings.Split(id, "/")
	if len(parts) != 2 {
		return PhysicalPartitionId{}, errors.New("could not parse a physical partition id from '" + id + "'")
	}

	return PhysicalPartitionId{
		PartitionName:         parts[0],
		PhysicalPartitionName: parts[1],
	}, nil
}

type RecipientsIds utils.Set[PhysicalPartitionId]

type ActorManagerFactory interface {
	BuildActorManager(actorId ActorId, runId string) ActorManager
}

type PhysicalPartitionManagerFactory interface {
	BuildPhyPartitionManager(phyPartitionId PhysicalPartitionId, runId string) PhysicalPartitionManager
}

type NotificationStorage interface {
	AddNotification(notification Notification) error
	RemoveNotification(notification Notification) error
	RemoveAllNotifications(notification ...Notification) error
	GetAllNotifications() []Notification
	Close() error
}

type NotificationStorageFactory interface {
	BuildNotificationStorage(identifier string) NotificationStorage
}

type Notification struct {
	PhyPartitionId PhysicalPartitionId
}

type CollectionId struct {
	Id       string
	TypeName string
}

func (cid CollectionId) GetTypeName() string {
	return cid.TypeName
}

type QueryableItem interface {
	GetId() string
	GetQueryableAttributes() map[string]string
}

type ItemCollector interface {
	GetDirtyItems() map[string]QueryableItem
	GetCollectionId() CollectionId
	CommitDirtyItems()
	DropDirtyItems()
}

type QueryableCollectionDao interface {
	GetItem(collectionId CollectionId, targetType reflect.Type, itemId string, context ExecutionContext) (any, error)
	FindItems(collectionId CollectionId, targetType reflect.Type, attributeName string, attributeValue string, context ExecutionContext) ([]any, error)
}

type QueryableCollectionDaoFactory interface {
	BuildQueryableCollectionDao() QueryableCollectionDao
}

type ExecutionContext struct {
	actorManager              ActorManager
	queryableCollectionDao    QueryableCollectionDao
	actorSpawningDao          ActorSpawningDao
	actorSpawnerConfig        ActorSpawnerConfig
	entityLoader              *EntityLoader
	timestampCollectorFactory TimestampCollectorFactory
	entityId                  string
	partitionName             string
	fieldName                 string
	runId                     string
}

func NewExecutionContext(
	queryableCollectionDao QueryableCollectionDao,
	actorSpawningDao ActorSpawningDao,
	actorSpawnerConfig ActorSpawnerConfig,
	entityLoader *EntityLoader,
	timestampCollectorFactory TimestampCollectorFactory,
	entityId string, partitionName string, runId string) ExecutionContext {
	return ExecutionContext{
		queryableCollectionDao:    queryableCollectionDao,
		actorSpawningDao:          actorSpawningDao,
		actorSpawnerConfig:        actorSpawnerConfig,
		entityLoader:              entityLoader,
		timestampCollectorFactory: timestampCollectorFactory,
		entityId:                  entityId,
		partitionName:             partitionName,
		runId:                     runId,
	}
}

type SpawningActorsCollector interface {
	GetSpawningActors() []Actor
}

type ActorSpawningDao interface {
	FetchPartition(partitionName PartitionName) (*FreshPartition, error)
	IncrementActorsCount(partitionName PartitionName, shardId ShardId) error
	AddShards(partitionName PartitionName, minNewShardsCount int) ([]*Shard, error)
	AddPartition(partitionName string) error
}

type ActorStorer interface {
	StoreActor(actor Actor) error
}

type MessageStorer interface {
	StoreMessage(payload string, receiver ActorId, uniqueSourceId string, seqNumber int, eventToken string) error
	AddActorTask(shardId PhysicalPartitionId) error
}

type ActorSpawningDaoFactory interface {
	BuildActorSpawningDao() ActorSpawningDao
}

type ActorSpawnerConfig struct {
	PartitionCacheValidInterval time.Duration
	MaxActorsPerShard           int
	MinNewShardsCount           int
}

type MessageCollector interface {
	GetAllOutboxes() []Outbox
}

type TimestampCollector interface {
	StartMeasurement(identifier string) error
	EndMeasurement(identifier string) error
}

type TimestampCollectorFactory interface {
	BuildTimestampCollector() TimestampCollector
}

type ExternalCommandHandler interface {
	SpawnActor(newActor Actor, partitionName string, instanceId string) (ActorId, error)
	SendMessage(payload string, receiver ActorId, uniqueSourceId string, seqNumber int, eventToken string) error
	CreatePartition(name string) error
	SpawnAggregator(aggregator Actor, partitionName string) error
}
