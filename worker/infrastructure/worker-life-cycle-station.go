package infrastructure

import (
	"time"
)

type WorkerLifeCycleStation struct {
	periodicSignal               <-chan time.Time
	newActorsCountingSignal      <-chan int
	releasedActorsCountingSignal <-chan int

	pullSignal chan<- int

	alreadyPullingActors bool
	activeActorsCount    int
	maxActiveActors      int

	idlePeriods int
}

func NewWorkerLifeCycleStation(periodicSignal <-chan time.Time, newActorsCountingSignal <-chan int, releasedActorsCountingSignal <-chan int, pullSignal chan<- int, maxActiveActors int) *WorkerLifeCycleStation {
	return &WorkerLifeCycleStation{periodicSignal: periodicSignal, newActorsCountingSignal: newActorsCountingSignal, releasedActorsCountingSignal: releasedActorsCountingSignal, pullSignal: pullSignal, maxActiveActors: maxActiveActors}
}

func (w *WorkerLifeCycleStation) Start() {
	w.alreadyPullingActors = true
	w.pullSignal <- w.maxActiveActors

	needsToQuit := false

	for {
		if needsToQuit {
			break
		}
		select {
		case <-w.periodicSignal:
			{
				if !w.alreadyPullingActors && w.activeActorsCount < w.maxActiveActors {
					w.pullSignal <- w.maxActiveActors - w.activeActorsCount
					w.alreadyPullingActors = true
				}
			}
		case newActorsCount := <-w.newActorsCountingSignal:
			{
				w.alreadyPullingActors = false

				if newActorsCount > 0 {
					w.activeActorsCount += newActorsCount
					w.idlePeriods = 0
				} else {
					w.idlePeriods++
					if w.idlePeriods > 5 {
						needsToQuit = true
					}
				}
			}
		case releasedActorsCount := <-w.releasedActorsCountingSignal:
			{
				w.activeActorsCount -= releasedActorsCount
			}
		}

	}
}
