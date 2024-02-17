package utils

import (
	"log"
	"math/rand"
	"time"
)

type Retrier[T any] struct {
	strategy HandlingStrategy
}

func NewRetrier[T any](strategy HandlingStrategy) *Retrier[T] {
	return &Retrier[T]{strategy: strategy}
}

func NewDefaultRetrier[T any]() *Retrier[T] {
	return NewRetrier[T](NewExponentialBackoffStrategy(-1, 50*time.Millisecond, 0.1, 2*time.Second))
}

func NewExponentialRetrierFactory[T any](maximumRetries int, initialDelay time.Duration, jitterPercentage float64, maxDelay time.Duration) func() *Retrier[T] {
	return func() *Retrier[T] {
		return NewRetrier[T](NewExponentialBackoffStrategy(maximumRetries, initialDelay, jitterPercentage, maxDelay))
	}
}

func NewNopRetrierFactory[T any]() func() *Retrier[T] {
	return func() *Retrier[T] {
		return NewRetrier[T](&NopRetryStrategy{})
	}
}

func (r *Retrier[T]) DoWithReturn(action func() (T, error)) (T, error) {
	var defaultT T
	if r.strategy.IsPreRequestDelayNeeded() {
		timeToWait := r.strategy.ComputePreRequestDelay()
		log.Printf("Recovering from errors. Waiting %v\n", timeToWait)
		time.Sleep(timeToWait)
	}
	for {
		result, err := action()
		if err == nil {
			r.strategy.HandleSuccess()
			return result, nil
		}
		decision := r.strategy.HandleError(err)
		if decision.ReturnError {
			return defaultT, err
		} else {
			log.Printf("Retrying due to error: %v. Time to wait: %v\n", err, decision.TimeToWait)
			time.Sleep(decision.TimeToWait)
		}
	}
}

type Decision struct {
	TimeToWait  time.Duration
	ReturnError bool
}

type HandlingStrategy interface {
	HandleError(err error) Decision
	HandleSuccess()
	IsPreRequestDelayNeeded() bool
	ComputePreRequestDelay() time.Duration
}

//NOT THREAD SAFE

type ExponentialBackoffStrategy struct {
	maximumRetries   int
	initialDelay     time.Duration
	maxDelay         time.Duration
	jitterPercentage float64

	currentRetryNumber int
	nextDelay          time.Duration
	rndGenerator       *rand.Rand

	recoveredFromFailures bool
}

func NewExponentialBackoffStrategy(maximumRetries int, initialDelay time.Duration, jitterPercentage float64, maxDelay time.Duration) *ExponentialBackoffStrategy {
	return &ExponentialBackoffStrategy{
		maximumRetries:        maximumRetries,
		initialDelay:          initialDelay,
		maxDelay:              maxDelay,
		jitterPercentage:      jitterPercentage,
		currentRetryNumber:    0,
		nextDelay:             initialDelay,
		rndGenerator:          rand.New(rand.NewSource(time.Now().UnixNano())),
		recoveredFromFailures: true,
	}
}

func (ebs *ExponentialBackoffStrategy) HandleError(err error) Decision {
	ebs.recoveredFromFailures = false
	if ebs.currentRetryNumber > ebs.maximumRetries && ebs.maximumRetries != -1 {
		return Decision{ReturnError: true}
	}
	currentDelay := ebs.nextDelay
	nextBaseDelay := ebs.nextDelay * 2
	if nextBaseDelay > ebs.maxDelay {
		nextBaseDelay = ebs.maxDelay
	} else {
		ebs.currentRetryNumber++
	}
	ebs.nextDelay = ebs.modifyWithJitter(nextBaseDelay)
	return Decision{TimeToWait: currentDelay}
}

func (ebs *ExponentialBackoffStrategy) HandleSuccess() {
	ebs.nextDelay /= 2
	ebs.currentRetryNumber = 0
	if ebs.nextDelay <= ebs.initialDelay {
		ebs.nextDelay = ebs.initialDelay
		ebs.recoveredFromFailures = true
	}
}

func (ebs *ExponentialBackoffStrategy) modifyWithJitter(duration time.Duration) time.Duration {
	maxJitterMilliseconds := int64(float64(duration.Milliseconds()) * ebs.jitterPercentage)
	jitterMilliseconds := ebs.rndGenerator.Int63n(maxJitterMilliseconds)
	jitterMilliseconds -= maxJitterMilliseconds / 2
	return duration + time.Duration(jitterMilliseconds)*time.Millisecond
}

func (ebs *ExponentialBackoffStrategy) ComputePreRequestDelay() time.Duration {
	return ebs.nextDelay
}

func (ebs *ExponentialBackoffStrategy) IsPreRequestDelayNeeded() bool {
	return !ebs.recoveredFromFailures
}

type NopRetryStrategy struct{}

func (nrs *NopRetryStrategy) HandleError(err error) Decision {
	return Decision{ReturnError: true}
}

func (nrs *NopRetryStrategy) HandleSuccess() {

}

func (nrs *NopRetryStrategy) IsPreRequestDelayNeeded() bool {
	return false
}

func (nrs *NopRetryStrategy) ComputePreRequestDelay() time.Duration {
	return time.Duration(0)
}
