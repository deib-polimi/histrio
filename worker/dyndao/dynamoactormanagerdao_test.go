package dyndao

/*
func _TestLoadFullActor(t *testing.T) {
	client := dynamoutils.CreateLocalClient()
	myDao := DynActorManagerDao{Client: client}

	state, inbox, err := myDao.LoadActor("MyActor/0", 100)

	if err != nil {
		t.Fatal(err)
	}

	if state == "" {
		t.Fatalf("Expected a non empty state, got '%v'", state)
	}

	if inbox.Size() == 0 {
		t.Fatalf("Expected a non empty inbox, got an empty one")
	}
}

*/
