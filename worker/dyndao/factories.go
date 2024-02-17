package dyndao

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/utils"
	"main/worker/domain"
	"time"
)

type ActorManagerFactoryImpl struct {
	Client                    *dynamodb.Client
	workerId                  string
	entityLoader              *domain.EntityLoader
	timestampCollectorFactory domain.TimestampCollectorFactory
}

func NewActorManagerFactoryImpl(client *dynamodb.Client, workerId string, entityLoader *domain.EntityLoader, timestampCollectorFactory domain.TimestampCollectorFactory) *ActorManagerFactoryImpl {
	return &ActorManagerFactoryImpl{Client: client, workerId: workerId, entityLoader: entityLoader, timestampCollectorFactory: timestampCollectorFactory}
}

func (f *ActorManagerFactoryImpl) BuildActorManager(actorId domain.ActorId, runId string) domain.ActorManager {
	actorManagerDao := NewDynActorManagerDao(f.Client, f.workerId)
	entityLoader := f.entityLoader
	collectionDao := NewDynQueryableCollectionDao(f.Client, entityLoader)
	actorSpawningDao := NewDynActorSpawningDao(f.Client)
	executionContext := domain.NewExecutionContext(
		collectionDao,
		actorSpawningDao,
		domain.ActorSpawnerConfig{
			PartitionCacheValidInterval: 15 * time.Second,
			MaxActorsPerShard:           50,
			MinNewShardsCount:           5,
		},
		entityLoader,
		f.timestampCollectorFactory,
		actorId.String(),
		actorId.PhyPartitionId.PhysicalPartitionName,
		runId,
	)
	return domain.NewActorManager(actorId, runId, actorManagerDao, entityLoader, executionContext)
}

type PhysicalPartitionManagerFactoryImpl struct {
	Client                    *dynamodb.Client
	WorkerId                  string
	entityLoader              *domain.EntityLoader
	timestampCollectorFactory domain.TimestampCollectorFactory
	retrierFactory            func() *utils.Retrier[struct{}]
}

func NewPhysicalPartitionManagerFactoryImpl(client *dynamodb.Client, workerId string, entityLoader *domain.EntityLoader, timestampCollectorFactory domain.TimestampCollectorFactory, retrierFactory func() *utils.Retrier[struct{}]) *PhysicalPartitionManagerFactoryImpl {
	return &PhysicalPartitionManagerFactoryImpl{Client: client, WorkerId: workerId, entityLoader: entityLoader, timestampCollectorFactory: timestampCollectorFactory, retrierFactory: retrierFactory}
}

func (f *PhysicalPartitionManagerFactoryImpl) BuildPhyPartitionManager(phyPartitionId domain.PhysicalPartitionId, runId string) domain.PhysicalPartitionManager {
	return domain.NewPhysicalPartitionManagerImpl(
		phyPartitionId,
		runId,
		NewDynPhysicalPartitionManagerDao(f.Client, f.WorkerId, f.entityLoader),
		NewActorManagerFactoryImpl(f.Client, f.WorkerId, f.entityLoader, f.timestampCollectorFactory),
		f.retrierFactory(),
	)
}

type DynQueryableCollectionDaoFactory struct {
	client       *dynamodb.Client
	entityLoader *domain.EntityLoader
}

func NewDynQueryableCollectionDaoFactory(client *dynamodb.Client, entityLoader *domain.EntityLoader) *DynQueryableCollectionDaoFactory {
	return &DynQueryableCollectionDaoFactory{client: client, entityLoader: entityLoader}
}

func (f *DynQueryableCollectionDaoFactory) BuildQueryableCollectionDao() domain.QueryableCollectionDao {
	return NewDynQueryableCollectionDao(f.client, f.entityLoader)
}
