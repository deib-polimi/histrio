package infrastructure

import (
	"log"
	"main/worker/domain"
	"time"
)

type ParkingStation struct {
	parkingQueue   <-chan domain.PhysicalPartitionManager
	periodicSignal <-chan time.Time

	newPhyPartitionQueue                chan<- domain.PhysicalPartitionManager
	releasedPhyPartitionsCountingSignal chan<- int

	parkingSlots              map[domain.PhysicalPartitionId]*parkingSlot
	passivationResultQueue    chan passivationResult
	passivationIntervalMillis int64
}

func NewParkingStation(parkingQueue <-chan domain.PhysicalPartitionManager, periodicSignal <-chan time.Time, newPhyPartitionQueue chan<- domain.PhysicalPartitionManager, releasedPhyPartitionsCountingSignal chan<- int, passivationIntervalMillis int64) *ParkingStation {
	return &ParkingStation{
		parkingQueue:                        parkingQueue,
		periodicSignal:                      periodicSignal,
		newPhyPartitionQueue:                newPhyPartitionQueue,
		releasedPhyPartitionsCountingSignal: releasedPhyPartitionsCountingSignal,
		parkingSlots:                        make(map[domain.PhysicalPartitionId]*parkingSlot),
		passivationIntervalMillis:           passivationIntervalMillis,
		passivationResultQueue:              make(chan passivationResult, 500),
	}
}

func (p *ParkingStation) Start() {
	go func() {
		for {
			select {

			case phyPartitionManager := <-p.parkingQueue:
				p.parkingSlots[phyPartitionManager.GetId()] = newParkingSlot(p.passivationResultQueue, phyPartitionManager)

			case result := <-p.passivationResultQueue:
				p.parkingSlots[result.phyPartitionId].isPassivating = false
				if result.successfullyParked {
					p.releasedPhyPartitionsCountingSignal <- 1
					log.Printf("Passivated shard %v\n", result.phyPartitionId)
					delete(p.parkingSlots, result.phyPartitionId)
				} else if !result.isQueueEmpty {
					p.newPhyPartitionQueue <- p.parkingSlots[result.phyPartitionId].phyPartitionManager
					delete(p.parkingSlots, result.phyPartitionId)
				}

			case <-p.periodicSignal:
				for _, slot := range p.parkingSlots {
					if !slot.isPassivating { //&& time.Since(slot.lastActivityTime) >= time.Duration(p.passivationIntervalMillis)*time.Millisecond {
						slot.isPassivating = true
						go slot.tryPassivate(time.Duration(p.passivationIntervalMillis) * time.Millisecond)
					}
				}
			}
		}
	}()
}

type parkingSlot struct {
	passivationResultQueue chan<- passivationResult

	phyPartitionManager domain.PhysicalPartitionManager
	isPassivating       bool
	lastActivityTime    time.Time
}

func newParkingSlot(passivationResultQueue chan<- passivationResult, phyPartitionManager domain.PhysicalPartitionManager) *parkingSlot {
	return &parkingSlot{
		passivationResultQueue: passivationResultQueue,
		phyPartitionManager:    phyPartitionManager,
		lastActivityTime:       time.Now(),
	}
}

func (ps *parkingSlot) tryPassivate(maxGraceDuration time.Duration) {
	var result passivationResult
	if time.Since(ps.lastActivityTime) > maxGraceDuration {
		ps.lastActivityTime = time.Now()
		isActorPassivated := ps.phyPartitionManager.TryPassivate()

		result = passivationResult{
			phyPartitionId:     ps.phyPartitionManager.GetId(),
			successfullyParked: isActorPassivated,
			isQueueEmpty:       ps.phyPartitionManager.GetActiveActorsCount() == 0,
		}
	} else {
		inboxesCount, err := ps.phyPartitionManager.FetchInboxes()
		if err != nil {
			result = passivationResult{
				phyPartitionId:     ps.phyPartitionManager.GetId(),
				successfullyParked: false,
				isQueueEmpty:       true,
			}
		} else {
			result = passivationResult{
				phyPartitionId:     ps.phyPartitionManager.GetId(),
				successfullyParked: false,
				isQueueEmpty:       inboxesCount == 0,
			}
		}
	}

	ps.passivationResultQueue <- result

}

type passivationResult struct {
	phyPartitionId     domain.PhysicalPartitionId
	successfullyParked bool
	isQueueEmpty       bool
}
