package infrastructure

import (
	"log"
	"main/worker/domain"
)

type PullingStation struct {
	pullRequestSignal <-chan ShardPullRequest

	readyQueue chan<- []domain.PhysicalPartitionManager

	taskDao                    domain.TaskDao
	phyPartitionManagerFactory domain.PhysicalPartitionManagerFactory
	workerId                   string
	runId                      string
}

func NewPullingStation(pullRequestSignal <-chan ShardPullRequest, readyQueue chan<- []domain.PhysicalPartitionManager, taskDao domain.TaskDao, phyPartitionManagerFactory domain.PhysicalPartitionManagerFactory, workerId string, runId string) *PullingStation {
	return &PullingStation{pullRequestSignal: pullRequestSignal, readyQueue: readyQueue, taskDao: taskDao, phyPartitionManagerFactory: phyPartitionManagerFactory, workerId: workerId, runId: runId}
}

func (ps *PullingStation) Start() {
	go func() {
		for {
			phyPartitionPollingRequest := <-ps.pullRequestSignal
			var pulledTasks []domain.ActorTask
			var err error
			if phyPartitionPollingRequest.isRecoveryRequest {
				pulledTasks, err = ps.taskDao.RecoverActorTasks(ps.workerId)
			} else {
				pulledTasks, err = ps.taskDao.PullNewActorTasks(ps.workerId, phyPartitionPollingRequest.maxShardsCount)
			}

			if err != nil {
				log.Printf("Error while polling: %v", err)
				continue
			}

			var newPhyPartitionManagers []domain.PhysicalPartitionManager
			for _, actorTask := range pulledTasks {
				newPhyPartitionManagers = append(newPhyPartitionManagers, ps.phyPartitionManagerFactory.BuildPhyPartitionManager(actorTask.PhyPartitionId, ps.runId))
			}

			ps.readyQueue <- newPhyPartitionManagers

		}
	}()
}
