package domain

type MessageSender struct {
	partitionName string

	collectedOutboxes map[PhysicalPartitionId]*Outbox
}

func (ms *MessageSender) Init(context ExecutionContext) {
	ms.partitionName = context.partitionName
	ms.collectedOutboxes = make(map[PhysicalPartitionId]*Outbox)
	context.actorManager.AddMessageCollector(ms)
}

func (ms *MessageSender) Tell(payload Message, receiver ActorId) {
	ms.collectMessage(payload, receiver)
}

func (ms *MessageSender) PublishEvent(payload Message) {
	collectorId := ActorId{"0", PhysicalPartitionId{PartitionName: ms.partitionName, PhysicalPartitionName: "0"}}
	ms.collectMessage(payload, collectorId)
}

func (ms *MessageSender) TellExternal(payload Message, externalId string) {
	destinationId := ActorId{
		InstanceId: externalId,
		PhyPartitionId: PhysicalPartitionId{
			PartitionName:         "-",
			PhysicalPartitionName: "-",
		},
	}
	ms.collectMessage(payload, destinationId)
}

func (ms *MessageSender) collectMessage(payload Message, receiver ActorId) {
	shardId := receiver.PhyPartitionId
	outbox, isOutboxAlreadyCreated := ms.collectedOutboxes[shardId]
	if !isOutboxAlreadyCreated {
		outbox = &Outbox{DestinationId: shardId}
		ms.collectedOutboxes[shardId] = outbox
	}

	outbox.AddMessage(MessageWithDestination{ActorId: receiver, Payload: payload})
}

func (ms *MessageSender) GetAllOutboxes() []Outbox {
	var outboxes []Outbox
	for _, outbox := range ms.collectedOutboxes {
		outboxes = append(outboxes, *outbox)
	}

	ms.collectedOutboxes = make(map[PhysicalPartitionId]*Outbox)
	return outboxes
}
