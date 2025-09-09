package domain

import "log"

type BenchmarkHelper struct {
	timestampCollector TimestampCollector
	runId              string
	startMeasure       []Measurement
	endMeasure         []Measurement
}

func (bh *BenchmarkHelper) Init(context ExecutionContext) {
	context.actorManager.AddBenchmarkHelper(bh)
	bh.timestampCollector = context.timestampCollectorFactory.BuildTimestampCollector()
	bh.runId = context.runId
	bh.startMeasure = make([]Measurement, 0)
	bh.endMeasure = make([]Measurement, 0)
}

func (bh *BenchmarkHelper) StartMeasurement(identifier string, message string) {
	bh.startMeasure = append(bh.startMeasure,
		Measurement{
			identifier:       identifier,
			message:          message,
			isEndMeasurement: false,
		})
}

func (bh *BenchmarkHelper) EndMeasurement(identifier string, message string) {
	bh.endMeasure = append(bh.endMeasure,
		Measurement{
			identifier:       identifier,
			message:          message,
			isEndMeasurement: true,
		})
}

func (bh *BenchmarkHelper) ExecuteStartMeasurements() {
	for _, measurement := range bh.startMeasure {
		measurementIdentifier := bh.runId + "/" + measurement.identifier

		err := bh.timestampCollector.StartMeasurement(measurementIdentifier)
		if err != nil {
			log.Printf("Encountered error while making start mesaurement (id = %v): %v\n", measurementIdentifier, err)
		}
	}
	bh.startMeasure = make([]Measurement, 0)
}

func (bh *BenchmarkHelper) ExecuteEndMeasurements() {
	for _, measurement := range bh.endMeasure {
		measurementIdentifier := bh.runId + "/" + measurement.identifier

		err := bh.timestampCollector.EndMeasurement(measurementIdentifier)
		if err != nil {
			log.Printf("Encountered error while making end mesaurement (id = %v): %v\n", measurementIdentifier, err)
		}
	}
	bh.endMeasure = make([]Measurement, 0)
}

func (bh *BenchmarkHelper) IsEmpty() bool {
	return len(bh.startMeasure) == 0 && len(bh.endMeasure) == 0
}

type Measurement struct {
	identifier       string
	message          string
	isEndMeasurement bool
}
