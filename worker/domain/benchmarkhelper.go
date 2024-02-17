package domain

import "log"

type BenchmarkHelper struct {
	timestampCollector TimestampCollector
	runId              string
	measurements       map[bool][]Measurement //true -> before transaction. false -> after transaction
}

func (bh *BenchmarkHelper) Init(context ExecutionContext) {
	context.actorManager.AddBenchmarkHelper(bh)
	bh.timestampCollector = context.timestampCollectorFactory.BuildTimestampCollector()
	bh.runId = context.runId
	bh.measurements = make(map[bool][]Measurement)
}

func (bh *BenchmarkHelper) StartMeasurement(identifier string, message string, measureBeforeTransaction bool) {
	bh.measurements[measureBeforeTransaction] = append(bh.measurements[measureBeforeTransaction],
		Measurement{
			identifier:               identifier,
			message:                  message,
			measureBeforeTransaction: measureBeforeTransaction,
			isEndMeasurement:         false,
		})
}

func (bh *BenchmarkHelper) EndMeasurement(identifier string, message string, measureBeforeTransaction bool) {
	bh.measurements[measureBeforeTransaction] = append(bh.measurements[measureBeforeTransaction],
		Measurement{
			identifier:               identifier,
			message:                  message,
			measureBeforeTransaction: measureBeforeTransaction,
			isEndMeasurement:         true,
		})
}

func (bh *BenchmarkHelper) ExecuteMeasurements(isBeforeTransaction bool) {

	for _, measurement := range bh.measurements[isBeforeTransaction] {
		measurementIdentifier := bh.runId + "/" + measurement.identifier
		if measurement.isEndMeasurement {
			err := bh.timestampCollector.EndMeasurement(measurementIdentifier)
			if err != nil {
				log.Printf("Encountered error while making end mesaurement (id = %v): %v\n", measurementIdentifier, err)
			}
		} else {
			err := bh.timestampCollector.StartMeasurement(measurementIdentifier)
			if err != nil {
				log.Printf("Encountered error while making start mesaurement (id = %v): %v\n", measurementIdentifier, err)
			}
		}
	}
	delete(bh.measurements, isBeforeTransaction)
}

func (bh *BenchmarkHelper) IsEmpty() bool {
	return len(bh.measurements) == 0
}

type Measurement struct {
	identifier               string
	message                  string
	measureBeforeTransaction bool
	isEndMeasurement         bool
}
