package domain

type Message interface{}

type ActorMessage struct {
	Id       MessageIdentifier
	SenderId ActorId
	Content  Message
}

type MessageIdentifier struct {
	ActorId         ActorId
	UniqueTimestamp string
}

type MessageWithDestination struct {
	ActorId ActorId
	Payload Message
}

type Outbox struct {
	DestinationId PhysicalPartitionId
	Messages      []MessageWithDestination
}

func (o *Outbox) AddMessage(m MessageWithDestination) {
	o.Messages = append(o.Messages, m)
}
