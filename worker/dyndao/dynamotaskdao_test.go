package dyndao

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

	if !slices.Contains(existingTableNames, "ActorTask") {
		dynamoutils.CreateActorTaskTable(client)
	}
}

func Cleanup(client *dynamodb.Client) {
	dynamoutils.DeleteTable(client, "ActorState")
	dynamoutils.DeleteTable(client, "ActorInbox")
	dynamoutils.DeleteTable(client, "ActorTask")
}

func TestPullSomeTasks(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	client := dynamoutils.CreateLocalClient()
	tasksCount := 10
	Setup(client)
	defer Cleanup(client)
	for i := range tasksCount {
		dynamoutils.AddActorTask(client, fmt.Sprintf("MyActor/%v", i), false, "NULL")
	}
	taskDao := DynTaskDao{Client: client}
	pulledTasks, err := taskDao.PullNewActorTasks("0", 10)
	if err != nil {
		t.Fatal(err)
	}

	if len(pulledTasks) != tasksCount {
		t.Fatalf("Expected tasks pulled: %v, actual: %v", tasksCount, len(pulledTasks))
	}
}
*/
