package domain

import (
	"encoding/json"
	"log"
	"main/utils"
	"time"
)

type PendingTransaction struct {
	speculatedState string
	consumedMessage ActorMessage
	outboxes        []Outbox
	dirtyItems      map[CollectionId][]QueryableItem
	spawningActors  []Actor
}

type ActorManagerImpl struct {
	actorId   ActorId
	actorType string
	runId     string

	isActorLoaded           bool
	actor                   Actor
	inbox                   *utils.Queue[ActorMessage]
	itemCollectors          map[CollectionId]ItemCollector
	spawningActorsCollector []SpawningActorsCollector
	messageCollectors       []MessageCollector
	benchmarkHelpers        []*BenchmarkHelper

	hasPendingTransaction bool
	pendingTransaction    PendingTransaction
	lastCommittedState    string

	actorManagerDao  ActorManagerDao
	entityLoader     *EntityLoader
	executionContext ExecutionContext
}

func NewActorManager(
	actorId ActorId, runId string, actorManagerDao ActorManagerDao, entityLoader *EntityLoader,
	executionContext ExecutionContext,
) *ActorManagerImpl {
	newActorManager := &ActorManagerImpl{
		actorId:          actorId,
		runId:            runId,
		actorManagerDao:  actorManagerDao,
		entityLoader:     entityLoader,
		executionContext: executionContext,
		inbox:            utils.NewQueue([]ActorMessage{}),
		itemCollectors:   make(map[CollectionId]ItemCollector),
	}

	newActorManager.executionContext.actorManager = newActorManager

	return newActorManager
}

func (mng *ActorManagerImpl) IsActorLoaded() bool {
	return mng.isActorLoaded
}

func (mng *ActorManagerImpl) IsQueueEmpty() bool {
	return mng.inbox.IsEmpty()
}

func (mng *ActorManagerImpl) GetPhyPartitionId() PhysicalPartitionId {
	return mng.actorId.PhyPartitionId
}

func (mng *ActorManagerImpl) GetActorId() ActorId {
	return mng.actorId
}

func (mng *ActorManagerImpl) ReplenishQueue(queue *utils.Queue[ActorMessage]) {
	mng.inbox.PushBackAll(queue.ToSlice()...)
}

func (mng *ActorManagerImpl) GetQueueSize() int {
	return mng.inbox.Size()
}

func (mng *ActorManagerImpl) LoadActor() error {
	currentState, actorType, err := mng.actorManagerDao.FetchState(mng.actorId)

	if err != nil {
		return err
	}

	deserializedActor, err := mng.entityLoader.LoadEntityByTypeName(currentState, actorType, mng.executionContext)
	if err != nil {
		log.Fatalf("FATAL: actor with id '%v' could not be deserialized due to: %v", mng.actorId.String(), err)
	}
	mng.actor = deserializedActor.(Actor)
	mng.actorType = actorType

	mng.isActorLoaded = true
	mng.lastCommittedState = currentState

	return nil
}

func (mng *ActorManagerImpl) PrepareMessageProcessing() (RecipientsIds, error) {
	if mng.inbox.IsEmpty() {
		return &utils.MapSet[PhysicalPartitionId]{}, nil
	}

	if !mng.IsActorLoaded() {
		err := mng.LoadActor()
		if err != nil {
			log.Fatalf("Could not load the actor state for actor %v", mng.GetActorId())
		}
	}

	nextMessage := mng.inbox.Pop()
	err := mng.actor.ReceiveMessage(nextMessage.Content)

	if err != nil {
		mng.ForceMessageProcessingRollback()
		return &utils.MapSet[PhysicalPartitionId]{}, err
	}

	serializedActorState, err := json.Marshal(mng.actor)

	if err != nil {
		mng.ForceMessageProcessingRollback()
		log.Fatalf("Cannot serialize actor with id %v\n", mng.actorId.String())
	}

	newState := string(serializedActorState)
	//collect all dirty items
	dirtyItems := make(map[CollectionId][]QueryableItem)
	for _, collector := range mng.itemCollectors {
		for _, item := range collector.GetDirtyItems() {
			dirtyItems[collector.GetCollectionId()] = append(dirtyItems[collector.GetCollectionId()], item)
		}
	}

	//collect all new actors spawned
	var spawningActors []Actor
	for _, spawningActorsCollector := range mng.spawningActorsCollector {
		for _, spawningActor := range spawningActorsCollector.GetSpawningActors() {
			spawningActors = append(spawningActors, spawningActor)
		}
	}
	//collect all messages sent by the actor
	var outboxes []Outbox
	for _, messageCollector := range mng.messageCollectors {
		for _, outbox := range messageCollector.GetAllOutboxes() {
			outboxes = append(outboxes, outbox)
		}
	}

	mng.pendingTransaction = PendingTransaction{
		speculatedState: newState,
		dirtyItems:      dirtyItems,
		spawningActors:  spawningActors,
		consumedMessage: nextMessage,
		outboxes:        outboxes,
	}

	mng.hasPendingTransaction = true

	destinations := utils.NewMapSet[PhysicalPartitionId]()

	for _, outbox := range outboxes {
		destinations.Add(outbox.DestinationId)
	}

	return destinations, nil

}

func (mng *ActorManagerImpl) CommitMessageProcessing() error {

	for _, benchmarkHelper := range mng.benchmarkHelpers {
		if !benchmarkHelper.IsEmpty() {
			executeMeasurement(benchmarkHelper, true)
		}
	}
	err := mng.actorManagerDao.ExecuteTransaction(
		mng.actorId,
		mng.pendingTransaction.speculatedState,
		mng.pendingTransaction.dirtyItems,
		mng.pendingTransaction.spawningActors,
		mng.pendingTransaction.consumedMessage,
		mng.pendingTransaction.outboxes,
		mng.runId,
	)

	if err != nil {
		mng.ForceMessageProcessingRollback()
		return err
	} else {
		for _, benchmarkHelper := range mng.benchmarkHelpers {
			if !benchmarkHelper.IsEmpty() {
				executeMeasurement(benchmarkHelper, false)
			}
		}
		mng.lastCommittedState = mng.pendingTransaction.speculatedState
		for _, collector := range mng.itemCollectors {
			collector.CommitDirtyItems()
		}
		mng.hasPendingTransaction = false
		return nil
	}
}

func (mng *ActorManagerImpl) ForceMessageProcessingRollback() {
	actorType := mng.actorType
	deserializedActor, err := mng.entityLoader.LoadEntityByTypeName(mng.lastCommittedState, actorType, mng.executionContext)
	if err != nil {
		log.Fatalf("FATAL: actor with id '%v' could not be deserialized after a rollback due to: %v", mng.actorId.String(), err)
	}
	mng.actor = deserializedActor.(Actor)
	mng.inbox.PushFront(mng.pendingTransaction.consumedMessage)
	for _, collector := range mng.itemCollectors {
		collector.DropDirtyItems()
	}
	mng.hasPendingTransaction = false
}

func (mng *ActorManagerImpl) AddItemCollector(collector ItemCollector) {
	mng.itemCollectors[collector.GetCollectionId()] = collector
}

func (mng *ActorManagerImpl) AddSpawningActorsCollector(collector SpawningActorsCollector) {
	mng.spawningActorsCollector = append(mng.spawningActorsCollector, collector)
}

func (mng *ActorManagerImpl) AddMessageCollector(collector MessageCollector) {
	mng.messageCollectors = append(mng.messageCollectors, collector)
}

func (mng *ActorManagerImpl) AddBenchmarkHelper(helper *BenchmarkHelper) {
	mng.benchmarkHelpers = append(mng.benchmarkHelpers, helper)
}

func executeMeasurement(benchmarkHelper *BenchmarkHelper, isBeforeTransaction bool) {
	startTime := time.Now()
	benchmarkHelper.ExecuteMeasurements(isBeforeTransaction)
	delta := time.Since(startTime)
	log.Printf("Measurement took %v ms", delta.Milliseconds())
}
