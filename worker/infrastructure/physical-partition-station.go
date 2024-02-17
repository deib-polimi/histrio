package infrastructure

import (
	"golang.org/x/exp/maps"
	"log"
	"main/utils"
	"main/worker/domain"
	"sort"
	"time"
)

type PhysicalPartitionStation struct {
	periodicSignal                     <-chan time.Time
	pollingSignal                      <-chan time.Time
	newPhyPartitionsQueue              <-chan []domain.PhysicalPartitionManager
	newPhyPartitionsFromParkingQueue   <-chan domain.PhysicalPartitionManager
	passivatedPhyPartitionsCountSignal <-chan int
	completedActorManagersQueue        <-chan domain.ActorManager
	idleQueue                          chan domain.PhysicalPartitionId
	activeActorsCountUpdateSignal      chan ActiveActorsUpdate

	processingQueue   chan<- domain.ActorManager
	parkingQueue      chan<- domain.PhysicalPartitionManager
	pullRequestSignal chan<- ShardPullRequest

	workerId string

	phyPartitionsLoci                            map[domain.PhysicalPartitionId]*physicalPartitionLocus
	maximumConcurrentShardPullsCount             int
	maximumActiveActors                          int
	minimumActiveActors                          int
	phyPartitionReleaseRequest                   physicalPartitionReleaseRequest
	isPullingRequestPending                      bool
	isRecoveryPhaseFinished                      bool
	totalActiveActors                            int
	totalParkedPhyPartitions                     int
	maximumSubsequentEmptyPhyPartitionsPullCount int
	subsequentEmptyPhyPartitionsPullCount        int
	useBackoffStrategy                           bool
	idleMillisecondsToWaitBeforeParking          int64
}

func NewPhysicalPartitionStation(
	periodicSignal <-chan time.Time, pollingSignal <-chan time.Time, newPhyPartitionsQueue <-chan []domain.PhysicalPartitionManager,
	newPhyPartitionsFromParkingQueue <-chan domain.PhysicalPartitionManager, passivatedPhyPartitionsCountSignal <-chan int,
	completedActorManagersQueue <-chan domain.ActorManager, processingQueue chan<- domain.ActorManager,
	parkingQueue chan<- domain.PhysicalPartitionManager, pullingStationSignal chan<- ShardPullRequest,
	workerId string,
	maximumConcurrentShardPullsCount int,
	maximumActiveActors int, minimumActiveActors int,
	maximumSubsequentEmptyPhyPartitionsPullCount int,
	useBackoffStrategy bool, idleMillisecondsToWaitBeforeParking int64) *PhysicalPartitionStation {
	return &PhysicalPartitionStation{
		periodicSignal:                               periodicSignal,
		pollingSignal:                                pollingSignal,
		newPhyPartitionsQueue:                        newPhyPartitionsQueue,
		newPhyPartitionsFromParkingQueue:             newPhyPartitionsFromParkingQueue,
		passivatedPhyPartitionsCountSignal:           passivatedPhyPartitionsCountSignal,
		completedActorManagersQueue:                  completedActorManagersQueue,
		idleQueue:                                    make(chan domain.PhysicalPartitionId, 1000),
		activeActorsCountUpdateSignal:                make(chan ActiveActorsUpdate, 10000),
		processingQueue:                              processingQueue,
		parkingQueue:                                 parkingQueue,
		pullRequestSignal:                            pullingStationSignal,
		phyPartitionsLoci:                            make(map[domain.PhysicalPartitionId]*physicalPartitionLocus),
		workerId:                                     workerId,
		maximumConcurrentShardPullsCount:             maximumConcurrentShardPullsCount,
		maximumActiveActors:                          maximumActiveActors,
		minimumActiveActors:                          minimumActiveActors,
		maximumSubsequentEmptyPhyPartitionsPullCount: maximumSubsequentEmptyPhyPartitionsPullCount,
		useBackoffStrategy:                           useBackoffStrategy,
		idleMillisecondsToWaitBeforeParking:          idleMillisecondsToWaitBeforeParking,
	}
}

func (ps *PhysicalPartitionStation) Start() {
	stop := false
	isRecovered := false
	isRecovering := false
	for {
		if stop {
			break
		}
		select {
		case newPhyPartitions := <-ps.newPhyPartitionsQueue:
			for _, newPhyPartition := range newPhyPartitions {
				ps.phyPartitionsLoci[newPhyPartition.GetId()] = newPhysicalPartitionLocus(newPhyPartition, ps.activeActorsCountUpdateSignal, ps.processingQueue, ps.idleQueue, ps.useBackoffStrategy, ps.idleMillisecondsToWaitBeforeParking)
				//log.Printf("PhyPartition %v entered the station\n", newPhyPartition.GetId().String())
			}
			ps.isPullingRequestPending = false
			isRecovering = false
			isRecovered = true
			if len(newPhyPartitions) == 0 {
				ps.subsequentEmptyPhyPartitionsPullCount++
			} else {
				ps.subsequentEmptyPhyPartitionsPullCount = 0
			}

		case newPhyPartition := <-ps.newPhyPartitionsFromParkingQueue:
			ps.phyPartitionsLoci[newPhyPartition.GetId()] = newPhysicalPartitionLocus(newPhyPartition, ps.activeActorsCountUpdateSignal, ps.processingQueue, ps.idleQueue, ps.useBackoffStrategy, ps.idleMillisecondsToWaitBeforeParking)
			ps.totalParkedPhyPartitions -= 1

		case passivationUpdate := <-ps.passivatedPhyPartitionsCountSignal:
			ps.totalParkedPhyPartitions -= passivationUpdate

		case completedActorManager := <-ps.completedActorManagersQueue:
			ps.phyPartitionsLoci[completedActorManager.GetPhyPartitionId()].slot.completedActorManagersQueue <- completedActorManager

		case activeActorsUpdate := <-ps.activeActorsCountUpdateSignal:
			locus, ok := ps.phyPartitionsLoci[activeActorsUpdate.phyPartitionId]
			if ok { // we might consume the deletion request first, so the locus might not be present anymore

				delta := activeActorsUpdate.actorsCount - locus.activeActorsCount
				ps.totalActiveActors += delta
				locus.activeActorsCount = activeActorsUpdate.actorsCount
			}

		case <-ps.periodicSignal:
			if isRecovered == false && isRecovering == false {
				ps.pullRequestSignal <- ShardPullRequest{isRecoveryRequest: true}
				isRecovering = true
			}
			desiredActorsCount := (ps.maximumActiveActors + ps.minimumActiveActors) / 2
			if ps.totalActiveActors > ps.maximumActiveActors {
				if !ps.phyPartitionReleaseRequest.isPending {
					ps.phyPartitionReleaseRequest = *newPhysicalPartitionReleaseRequest(maps.Values(ps.phyPartitionsLoci), ps.totalActiveActors-desiredActorsCount)
					for _, id := range ps.phyPartitionReleaseRequest.getAllRemainingPhyPartitionIds() {
						select {
						case ps.phyPartitionsLoci[id].slot.terminationSignal <- struct{}{}:
						default:
						}
					}
				}
			}

			if ps.totalActiveActors < ps.minimumActiveActors && isRecovered {
				ps.pullRequestSignal <- ShardPullRequest{maxShardsCount: ps.maximumConcurrentShardPullsCount}
				ps.isPullingRequestPending = true
			}

			if ps.totalActiveActors == 0 && ps.totalParkedPhyPartitions == 0 && len(ps.phyPartitionsLoci) == 0 && ps.subsequentEmptyPhyPartitionsPullCount >= ps.maximumSubsequentEmptyPhyPartitionsPullCount {
				stop = true
			}

		case tPolling := <-ps.pollingSignal:
			//send the request to poll from all inboxes
			for _, locus := range ps.phyPartitionsLoci {
				select {
				case locus.slot.periodicSignal <- tPolling:
				default:
				}
			}

		case phyPartitionId := <-ps.idleQueue:
			phyPartitionManager := ps.phyPartitionsLoci[phyPartitionId].slot.phyPartitionManager
			doesPhyPartitionNeedToBeParked := true
			if ps.phyPartitionReleaseRequest.isPending {
				doesPhyPartitionNeedToBeParked = !ps.phyPartitionReleaseRequest.contains(phyPartitionManager.GetId())
				ps.phyPartitionReleaseRequest.processPartitionRelease(phyPartitionManager.GetId())
			}

			if doesPhyPartitionNeedToBeParked {
				ps.parkingQueue <- phyPartitionManager
				ps.totalParkedPhyPartitions += 1
			} else {
				err := phyPartitionManager.Release(ps.workerId)
				if err != nil {
					log.Fatalf("Could not release the shard '%v': %v\n", phyPartitionId.String(), err)
				}
			}
			ps.totalActiveActors -= ps.phyPartitionsLoci[phyPartitionManager.GetId()].activeActorsCount
			delete(ps.phyPartitionsLoci, phyPartitionManager.GetId())

		}
	}
}

func (ps *PhysicalPartitionStation) getRunningPhyPartitions() int {
	return len(ps.phyPartitionsLoci)
}

type physicalPartitionLocus struct {
	slot                *physicalPartitionSlot
	activeActorsCount   int
	allocationTimestamp time.Time
}

func newPhysicalPartitionLocus(phyManager domain.PhysicalPartitionManager,
	activeActorsCountUpdateSignal chan<- ActiveActorsUpdate, processingQueue chan<- domain.ActorManager,
	idleQueue chan<- domain.PhysicalPartitionId, useBackoffStrategy bool, idleMillisecondsToWaitBeforeParking int64) *physicalPartitionLocus {
	slot := newPhysicalPartitionSlot(phyManager, activeActorsCountUpdateSignal, processingQueue, idleQueue, useBackoffStrategy, idleMillisecondsToWaitBeforeParking)
	slot.Start()
	return &physicalPartitionLocus{
		slot:                slot,
		allocationTimestamp: time.Now(),
	}
}

type physicalPartitionLociOrdered []*physicalPartitionLocus

func (p physicalPartitionLociOrdered) Len() int {
	return len(p)
}
func (p physicalPartitionLociOrdered) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}
func (p physicalPartitionLociOrdered) Less(i, j int) bool {
	return p[i].allocationTimestamp.Before(p[j].allocationTimestamp)
}

type physicalPartitionReleaseRequest struct {
	partitionsToRelease utils.Set[domain.PhysicalPartitionId]
	isPending           bool
}

func newPhysicalPartitionReleaseRequest(phyPartitionsLoci []*physicalPartitionLocus, actorsToRemove int) *physicalPartitionReleaseRequest {
	phyPartitionsToRelease := utils.NewMapSet[domain.PhysicalPartitionId]()

	sort.Sort(physicalPartitionLociOrdered(phyPartitionsLoci))
	for _, locus := range phyPartitionsLoci {
		if actorsToRemove <= 0 {
			break
		}
		phyPartitionsToRelease.Add(locus.slot.phyPartitionManager.GetId())
		actorsToRemove -= locus.activeActorsCount

	}

	return &physicalPartitionReleaseRequest{
		partitionsToRelease: phyPartitionsToRelease,
		isPending:           true,
	}
}

func (p *physicalPartitionReleaseRequest) getAllRemainingPhyPartitionIds() []domain.PhysicalPartitionId {
	return p.partitionsToRelease.ToSlice()
}

func (p *physicalPartitionReleaseRequest) processPartitionRelease(id domain.PhysicalPartitionId) {
	p.partitionsToRelease.Remove(id)
	if p.partitionsToRelease.GetSize() == 0 {
		p.isPending = false
	}
}

func (p *physicalPartitionReleaseRequest) contains(id domain.PhysicalPartitionId) bool {
	return p.partitionsToRelease.Contains(id)
}

type physicalPartitionSlot struct {
	completedActorManagersQueue chan domain.ActorManager
	periodicSignal              chan time.Time
	terminationSignal           chan struct{}

	processingQueue               chan<- domain.ActorManager
	activeActorsCountUpdateSignal chan<- ActiveActorsUpdate
	idleQueue                     chan<- domain.PhysicalPartitionId

	useBackoffStrategy bool

	phyPartitionManager domain.PhysicalPartitionManager
	backoffCyclesToWait int //phyPartition needs to wait for backoffCyclesToWait periodic signals before polling
	lastBackoffDelay    int
	needsToStop         bool //external system asked this phyPartitionSlot to stop processing
	activeActorsCount   int

	lastMessageProcessedTime            time.Time
	lastActiveActorsCountUpdateValue    int
	idleMillisecondsToWaitBeforeParking int64
}

func newPhysicalPartitionSlot(
	phyPartitionManager domain.PhysicalPartitionManager, activeActorsCountUpdateSignal chan<- ActiveActorsUpdate,
	processingQueue chan<- domain.ActorManager, idleQueue chan<- domain.PhysicalPartitionId, useBackoffStrategy bool,
	idleMillisecondsToWaitBeforeParking int64) *physicalPartitionSlot {
	return &physicalPartitionSlot{
		completedActorManagersQueue:         make(chan domain.ActorManager, 10000),
		periodicSignal:                      make(chan time.Time),
		terminationSignal:                   make(chan struct{}),
		processingQueue:                     processingQueue,
		activeActorsCountUpdateSignal:       activeActorsCountUpdateSignal,
		idleQueue:                           idleQueue,
		phyPartitionManager:                 phyPartitionManager,
		lastBackoffDelay:                    1,
		lastMessageProcessedTime:            time.Now(),
		useBackoffStrategy:                  useBackoffStrategy,
		lastActiveActorsCountUpdateValue:    -1000,
		idleMillisecondsToWaitBeforeParking: idleMillisecondsToWaitBeforeParking,
	}
}

func (ps *physicalPartitionSlot) Start() {
	go func() {
		stop := false
		for {
			if stop {
				break
			}

			select {
			case <-ps.periodicSignal: //polling inboxes and check for phyPartition termination
				canPoll := true
				if ps.useBackoffStrategy {
					canPoll = !ps.needsToStop && ps.backoffCyclesToWait == 0
				} else {
					canPoll = !ps.needsToStop
				}
				if canPoll {
					newMessagesPolled, err := ps.phyPartitionManager.FetchInboxes()

					if err != nil {
						break
					}

					if newMessagesPolled == 0 {
						ps.lastBackoffDelay *= 2
						ps.backoffCyclesToWait = ps.lastBackoffDelay
					} else {
						ps.lastMessageProcessedTime = time.Now()
					}
				} else {
					ps.backoffCyclesToWait--
				}

				for _, actorManager := range ps.phyPartitionManager.PopReadyActorManagers() {
					ps.processingQueue <- actorManager
				}

				actorsCount := ps.phyPartitionManager.GetActiveActorsCount()
				if actorsCount != ps.lastActiveActorsCountUpdateValue {
					ps.activeActorsCountUpdateSignal <- ActiveActorsUpdate{actorsCount: actorsCount, phyPartitionId: ps.phyPartitionManager.GetId()}
					ps.lastActiveActorsCountUpdateValue = actorsCount
				}

				if actorsCount == 0 && time.Now().Sub(ps.lastMessageProcessedTime).Milliseconds() > ps.idleMillisecondsToWaitBeforeParking {
					stop = true
				}

			case completedActorManager := <-ps.completedActorManagersQueue:

				ps.phyPartitionManager.AcceptCompletedActorManager(completedActorManager)

				for _, actorManager := range ps.phyPartitionManager.PopReadyActorManagers() {
					ps.processingQueue <- actorManager
				}

				if ps.needsToStop && ps.phyPartitionManager.GetActiveActorsCount() == 0 {
					stop = true
					break
				}

			case <-ps.terminationSignal:
				ps.needsToStop = true

			}

		}

		ps.idleQueue <- ps.phyPartitionManager.GetId()
	}()
}

type ActiveActorsUpdate struct {
	phyPartitionId domain.PhysicalPartitionId
	actorsCount    int
}

type ShardPullRequest struct {
	isRecoveryRequest bool
	maxShardsCount    int
}
