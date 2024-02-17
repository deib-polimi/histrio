package domain

type ExternalHandler struct {
	actorSpawner  *ActorSpawner
	actorStorer   ActorStorer
	messageStorer MessageStorer
}

func NewExternalHandler(actorSpawner *ActorSpawner, actorStorer ActorStorer, messageStorer MessageStorer) *ExternalHandler {
	return &ExternalHandler{actorSpawner: actorSpawner, actorStorer: actorStorer, messageStorer: messageStorer}
}

func (eh *ExternalHandler) SpawnActor(newActor Actor, partitionName string, instanceId string) (ActorId, error) {
	actorId, err := eh.actorSpawner.Spawn(newActor, partitionName, instanceId)

	if err != nil {
		return ActorId{}, err
	}

	newActor.SetId(actorId)
	err = eh.actorStorer.StoreActor(newActor)

	if err != nil {
		return ActorId{}, err
	}

	return actorId, err
}

func (eh *ExternalHandler) SpawnAggregator(aggregator Actor, partitionName string) error {
	actorId := ActorId{
		PhyPartitionId: PhysicalPartitionId{PartitionName: partitionName, PhysicalPartitionName: "0"},
		InstanceId:     "0",
	}

	aggregator.SetId(actorId)
	return eh.actorStorer.StoreActor(aggregator)
}

func (eh *ExternalHandler) SendMessage(payload string, receiver ActorId, uniqueSourceId string, seqNumber int, eventToken string) error {
	err := eh.messageStorer.StoreMessage(payload, receiver, uniqueSourceId, seqNumber, eventToken)
	if err != nil {
		return err
	}

	err = eh.messageStorer.AddActorTask(receiver.PhyPartitionId)

	return err
}

func (eh *ExternalHandler) CreatePartition(name string) error {
	return eh.actorSpawner.AddPartition(name)
}
