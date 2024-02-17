package integration

/*

func Setup(client *dynamodb.Client) {
	existingTableNames, err := dynamoutils.GetExistingTableNames(client)

	if err != nil {
		log.Fatal(err)
	}

	if !slices.Contains(existingTableNames, "ActorState") {
		dynamoutils.CreateActorStateTable(client)
	}

	if !slices.Contains(existingTableNames, "ActorInbox") {
		dynamoutils.CreateActorInboxTable(client)
	}

}

func Cleanup(client *dynamodb.Client) {
	dynamoutils.DeleteTable(client, "ActorState")
	dynamoutils.DeleteTable(client, "ActorInbox")
}

func TestPopulateOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	Setup(client)

	myActor := actor.MyActor{CurrentState: "Init"}
	dynamoutils.AddActorState(client, "MyActor/0", myActor, "0")
	dynamoutils.AddMessage(client, "actor2", "MyActor/0", "||Msg1||")
	dynamoutils.AddMessage(client, "actor2", "MyActor/0", "END")

}

func TestProcessOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}

	client := dynamoutils.CreateLocalClient()
	dynActorManagerDao := dyndao.DynActorManagerDao{Client: client}

	actorManager := actor.NewActorManager(&dynActorManagerDao, &actor.ActorLoader{})

	actorManager.LoadActor("MyActor/0", 100)

	actorManager.ConsumeMessage()
	actorManager.ProcessPreviousTransaction()
	actorManager.ConsumeMessage()
	actorManager.ProcessPreviousTransaction()

}

func TestCleanupOnly(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	client := dynamoutils.CreateLocalClient()
	Cleanup(client)

}

func TestProcessTwoMessages(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	TestPopulateOnly(t)
	TestProcessOnly(t)
	TestCleanupOnly(t)

}

*/
