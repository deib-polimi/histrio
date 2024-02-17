package domain

import (
	"log"
	"main/utils"
)

type PhysicalPartitionManagerImpl struct {
	id                  PhysicalPartitionId
	runId               string
	fetchedActorInboxes map[ActorId]*utils.Queue[ActorMessage]
	loadedActorManagers map[ActorId]ActorManager
	processingActorIds  utils.Set[ActorId]

	dao                 PhyPartitionManagerDao
	actorManagerFactory ActorManagerFactory
	retrier             *utils.Retrier[struct{}]
}

func NewPhysicalPartitionManagerImpl(id PhysicalPartitionId, runId string, dao PhyPartitionManagerDao, actorManagerFactory ActorManagerFactory, retrier *utils.Retrier[struct{}]) *PhysicalPartitionManagerImpl {
	return &PhysicalPartitionManagerImpl{
		id:                  id,
		runId:               runId,
		fetchedActorInboxes: make(map[ActorId]*utils.Queue[ActorMessage]),
		loadedActorManagers: make(map[ActorId]ActorManager),
		processingActorIds:  utils.NewMapSet[ActorId](),
		dao:                 dao,
		actorManagerFactory: actorManagerFactory,
		retrier:             retrier,
	}
}

func (pp *PhysicalPartitionManagerImpl) GetId() PhysicalPartitionId {
	return pp.id
}

func (pp *PhysicalPartitionManagerImpl) GetActiveActorsCount() int {
	actorsWithNewMessages := utils.NewMapSet[ActorId]()
	for actorId, queue := range pp.fetchedActorInboxes {
		if queue.Size() > 0 {
			actorsWithNewMessages.Add(actorId)
		}
	}

	actorsWithNewMessages.AddAll(pp.processingActorIds.ToSlice()...)

	return actorsWithNewMessages.GetSize()
}

func (pp *PhysicalPartitionManagerImpl) GetInboxesSize() int {
	messagesCount := 0
	for _, inbox := range pp.fetchedActorInboxes {
		messagesCount += inbox.Size()
	}

	return messagesCount
}

func (pp *PhysicalPartitionManagerImpl) FetchInboxes() (int, error) {
	newMessages, err := pp.dao.FetchNewMessagesFromInbox(pp.GetId())
	if err != nil {
		log.Printf("Error while fetching new messages from inbox %v: %v\n", pp.GetId().String(), err)
		return 0, err
	}

	//log.Printf("Shard '%v' polled %v messages from the inbox\n", pp.GetId().String(), len(newMessages))

	for _, message := range newMessages {
		actorId := message.Id.ActorId
		inbox, ok := pp.fetchedActorInboxes[actorId]
		if !ok {
			inbox = utils.NewQueue([]ActorMessage{})
			pp.fetchedActorInboxes[actorId] = inbox
			pp.loadedActorManagers[actorId] = pp.actorManagerFactory.BuildActorManager(actorId, pp.runId)
		}

		inbox.PushBack(message)
	}
	return len(newMessages), nil
}

func (pp *PhysicalPartitionManagerImpl) PopReadyActorManagers() []ActorManager {
	var poppedActorManagers []ActorManager
	for _, readyActorId := range pp.getReadyManagers() {
		readyManager := pp.loadedActorManagers[readyActorId]
		readyManager.ReplenishQueue(pp.fetchedActorInboxes[readyActorId])
		pp.processingActorIds.Add(readyActorId)
		pp.fetchedActorInboxes[readyActorId].Clear()
		poppedActorManagers = append(poppedActorManagers, readyManager)
	}

	return poppedActorManagers
}

func (pp *PhysicalPartitionManagerImpl) AcceptCompletedActorManager(actorManager ActorManager) {
	pp.processingActorIds.Remove(actorManager.GetActorId())
}

func (pp *PhysicalPartitionManagerImpl) getReadyManagers() []ActorId {
	var readyManagers []ActorId
	for actorId, queue := range pp.fetchedActorInboxes {
		if queue.Size() > 0 && !pp.processingActorIds.Contains(actorId) {
			readyManagers = append(readyManagers, actorId)
		}
	}

	return readyManagers
}

func (pp *PhysicalPartitionManagerImpl) TryPassivate() bool {
	err := pp.dao.SealPhysicalPartition(pp.GetId())

	if err != nil {
		return false
	}

	messagesFetchedCount, err := pp.FetchInboxes()
	if err != nil {
		pp.unseal()
		return false
	}

	if messagesFetchedCount > 0 {
		pp.unseal()
		return false
	}

	err = pp.dao.DeleteActorTask(pp.GetId())

	if err != nil {
		pp.unseal()
		return false
	}

	return true
}

func (pp *PhysicalPartitionManagerImpl) Release(workerId string) error {
	return pp.dao.ReleasePhyPartition(pp.id, workerId)
}

func (pp *PhysicalPartitionManagerImpl) unseal() {
	_, unsealError := pp.retrier.DoWithReturn(func() (struct{}, error) {
		return struct{}{}, pp.dao.UnsealPhysicalPartition(pp.GetId())
	})
	if unsealError != nil {
		log.Fatalf("Could not unseal the partition '%v' after having sealed it: %v", pp.GetId(), unsealError)
	}
}
