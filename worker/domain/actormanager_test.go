package domain

/*
type ActorManagerDaoMock struct {
	lastCommittedState  string
	lastCommittedOutbox []Outbox
}

func (mng *ActorManagerDaoMock) FetchState(actorId string) (state string, err error) {
	return "{\"CurrentState\": \"Init\"}", nil
}
func (mng *ActorManagerDaoMock) FetchQueue(actorId string) (inbox *utils.Queue[ActorMessage], err error) {
	return utils.NewQueue([]ActorMessage{{Id: MessageIdentifier{}, Content: "END"}}), nil
}
func (mng *ActorManagerDaoMock) ExecuteTransaction(actorId string, newState string, consumedMessage ActorMessage, outboxes []Outbox) error {
	mng.lastCommittedState = newState
	mng.lastCommittedOutbox = outboxes
	return nil
}

func (mng *ActorManagerDaoMock) LoadActor(actorId string, messagesToPullCount uint) (state string, inbox utils.Queue[ActorMessage], err error) {
	return "{\"CurrentState\": \"Init\"}", *utils.NewQueue([]ActorMessage{{Id: MessageIdentifier{}, Content: "END"}}), nil
}

func TestCreateManager(t *testing.T) {
	myManager := NewActorManager("actorId", &ActorManagerDaoMock{}, &EntityLoader{})
	if myManager.IsActorLoaded() {
		t.Fatalf("Newly created manager has a loaded actor")
	}
}

func TestLoadActor(t *testing.T) {
	myManager := NewActorManager("actorId", &ActorManagerDaoMock{}, &EntityLoader{})
	err := myManager.LoadActor()
	if err != nil {
		t.Fatal(err)
	}
	loadedActor := myManager.actor.(*MyActor)
	if myManager.actorId != "actorId" {
		t.Fatalf("Tried to load actor with id 'actorId', but loaded actor with id '%v'", myManager.actorId)
	}

	if loadedActor.CurrentState == "" {
		t.Fatalf("Actor loaded with empty state")
	}
}

func TestConsumeMessage(t *testing.T) {
	myManager := NewActorManager("actorId", &ActorManagerDaoMock{}, &EntityLoader{})
	err := myManager.LoadActor()
	if err != nil {
		t.Fatal(err)
	}
	_, err = myManager.FetchQueue()
	if err != nil {
		t.Fatal(err)
	}
	_, err = myManager.PrepareMessageProcessing()
	if err != nil {
		t.Fatal(err)
	}

	if myManager.hasPendingTransaction == false {
		t.Fatal("Transaction not yet processed, but hasPendingTransaction is false")
	}
}

func TestConsumeMessageAndProcessTransaction(t *testing.T) {

	myManager := NewActorManager("actorId", &ActorManagerDaoMock{}, &EntityLoader{})
	err := myManager.LoadActor()
	if err != nil {
		t.Fatal(err)
	}
	_, err = myManager.FetchQueue()
	if err != nil {
		t.Fatal(err)
	}
	_, err = myManager.PrepareMessageProcessing()
	if err != nil {
		t.Fatal(err)
	}

	err = myManager.CommitMessageProcessing()
	if err != nil {
		t.Fatal(err)
	}
	mockDao := myManager.actorManagerDao.(*ActorManagerDaoMock)

	if mockDao.lastCommittedState != "{\"Id\":\"\",\"CurrentState\":\"InitEND\"}" {
		t.Fatalf("Expected stored state: {\"Id\":\"\",\"CurrentState\":\"InitEND\"}. Actual: %v", mockDao.lastCommittedState)
	}

	if myManager.hasPendingTransaction {
		t.Fatal("Manager has a pending transaction when it should not have it")
	}

}
*/
