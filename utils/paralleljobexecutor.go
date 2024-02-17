package utils

import "sync"

type ParallelJobExecutor interface {
	RegisterConsumer(matcher func(tag string) bool, consumer Consumer)
	RegisterErrorHandler(handler func(err error))
	Start()
	SubmitJob(job func() (Result, error))
	Stop()
}

type Consumer interface {
	Consume(Result)
}

type Result struct {
	data any
	tag  string
}

type ParallelJobExecutorImpl struct {
	matchersWithConsumerChannel []Pair[func(string) bool, chan Result]
	maxParallelUnits            int
	jobExecutorsWg              sync.WaitGroup
	resultAndErrorHandlersWg    sync.WaitGroup
	jobQueue                    chan func() (Result, error)
	errorQueue                  chan error
}

func NewSimpleParallelJobExecutor(maxParallelUnits int) *ParallelJobExecutorImpl {
	return &ParallelJobExecutorImpl{
		maxParallelUnits: maxParallelUnits,
		jobQueue:         make(chan func() (Result, error), 1000),
		errorQueue:       make(chan error, 100),
	}
}

func (ex *ParallelJobExecutorImpl) RegisterConsumer(matcher func(tag string) bool, consumer Consumer) {
	ex.resultAndErrorHandlersWg.Add(1)
	consumerQueue := make(chan Result, 1000)
	ex.matchersWithConsumerChannel = append(ex.matchersWithConsumerChannel, Pair[func(string) bool, chan Result]{First: matcher, Second: consumerQueue})
	go func() {
		for res := range consumerQueue {
			consumer.Consume(res)
		}
		ex.resultAndErrorHandlersWg.Done()
	}()
}

func (ex *ParallelJobExecutorImpl) RegisterErrorHandler(handler func(err error)) {
	ex.resultAndErrorHandlersWg.Add(1)
	go func() {
		for myErr := range ex.errorQueue {
			handler(myErr)
		}
		ex.resultAndErrorHandlersWg.Done()
	}()
}

func (ex *ParallelJobExecutorImpl) Start() {
	for range ex.maxParallelUnits {
		ex.jobExecutorsWg.Add(1)
		go func() {
			for job := range ex.jobQueue {
				myResult, err := job()
				if err != nil {
					ex.errorQueue <- err
				} else {
					for _, matchAndChannelPair := range ex.matchersWithConsumerChannel {
						if matchAndChannelPair.First(myResult.tag) {
							matchAndChannelPair.Second <- myResult
						}
					}
				}
			}

			ex.jobExecutorsWg.Done()
		}()
	}
}

func (ex *ParallelJobExecutorImpl) SubmitJob(job func() (Result, error)) {
	ex.jobQueue <- job
}

func (ex *ParallelJobExecutorImpl) Stop() {
	close(ex.jobQueue)

	ex.jobExecutorsWg.Wait()

	for _, matchAndChannelPair := range ex.matchersWithConsumerChannel {
		close(matchAndChannelPair.Second)
	}

	close(ex.errorQueue)

	ex.resultAndErrorHandlersWg.Wait()
}
