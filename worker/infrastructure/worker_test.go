package infrastructure

/*

type MockActorManager struct {
	actorId string
	inboxSize int

	t *testing.T
}

func (m *MockActorManager) IsActorLoaded() bool {return !(m.actorId == "")}
func (m *MockActorManager) IsTaskExhausted() bool {return m.inboxSize == 0}
func (m *MockActorManager) GetActorId() string { return m.actorId}
func (m *MockActorManager) LoadActor(actorId string, minMessagesToConsumeCount uint) {
	m.actorId = actorId
	m.inboxSize = 2
}
func (m *MockActorManager) UnloadActor() {
	m.actorId = ""
	m.inboxSize = 0
}
func (m *MockActorManager) ProcessPreviousTransaction() {}
func (m *MockActorManager) ConsumeMessage() {m.inboxSize--}


type MockActorManagerFactory struct {
	t *testing.T
}
func (m *MockActorManagerFactory) BuildActorManager() ActorManager {
	return &MockActorManager{t:m.t}
}


type MockWorkerDao struct {
	allTasks []ActorTask
	consumedTasks int

	t *testing.T
}

func newMockWorkerDao(t *testing.T) *MockWorkerDao {
	allTasks := []ActorTask{}
	for i := range 10 {
		allTasks = append(allTasks, ActorTask{ActorId: "MyActor/" + strconv.Itoa(i), MessagesToConsumeCount: 50})
	}

	return &MockWorkerDao{allTasks: allTasks, consumedTasks: 0, t:t}
}

func (m *MockWorkerDao) PullNewActorTasks(workerId string) ([]ActorTask, error) {
	remainingTasks := m.allTasks[0:len(m.allTasks) / 2]
	newTasks := m.allTasks[len(m.allTasks) / 2 :]

	m.allTasks = remainingTasks
	return newTasks, nil
}

func (m *MockWorkerDao) ReleaseTask(schedulerId string, actorId string) error {
	m.consumedTasks++
	return nil
}

func TestCreateWorker(t *testing.T) {
	workerDao := newMockWorkerDao(t)
	actorManagerFactory := &MockActorManagerFactory{t:t}

	worker := NewWorker(100, workerDao, actorManagerFactory)
	worker.Run()

	if workerDao.consumedTasks != 10 {
		t.Fatalf("Expected to consume %v tasks, but consumed %v", 10, workerDao.consumedTasks)
	}

}

*/
