package benchmark

import (
	"hash/crc32"
	"log"
	"os"
	"path"
	"strconv"
	"sync"
	"time"
)

type RequestTimeLogger interface {
	LogStartRequest(identifier string) error
	LogEndRequest(identifier string) error
}

type RequestTimeLoggerImpl struct {
	concurrentLogsCount int
	loggerRoutines      []*TimeLoggerRoutine
	basePath            string
	wg                  *sync.WaitGroup
}

func NewRequestTimeLoggerImpl(baseLogPath string, logGroupName string, concurrentLogsCount int) *RequestTimeLoggerImpl {
	basePath := path.Join(baseLogPath, logGroupName)
	var wg sync.WaitGroup
	err := os.MkdirAll(basePath, os.ModePerm)
	if err != nil {
		log.Fatalf("Could not create the time logger: %v\n", err)
	}
	var loggerRoutines []*TimeLoggerRoutine
	for i := range concurrentLogsCount {
		wg.Add(1)
		timeLogger := NewTimeLogger(basePath, strconv.Itoa(i))
		loggerRoutines = append(loggerRoutines, NewTimeLoggerRoutine(timeLogger, &wg))
	}

	return &RequestTimeLoggerImpl{
		concurrentLogsCount: concurrentLogsCount,
		loggerRoutines:      loggerRoutines,
		basePath:            basePath,
		wg:                  &wg,
	}

}

func (tl *RequestTimeLoggerImpl) LogStartRequest(identifier string) error {
	tl.dispatch(Request{
		identifier:  identifier,
		requestType: START,
		timestamp:   time.Now(),
	})

	return nil
}

func (tl *RequestTimeLoggerImpl) LogEndRequest(identifier string) error {
	tl.dispatch(Request{
		identifier:  identifier,
		requestType: END,
		timestamp:   time.Now(),
	})

	return nil
}

func (tl *RequestTimeLoggerImpl) Start() {
	for _, routine := range tl.loggerRoutines {
		go routine.Start()
	}
}

func (tl *RequestTimeLoggerImpl) Stop() {
	for _, routine := range tl.loggerRoutines {
		routine.Close()
	}
	tl.wg.Wait()
}

func (tl *RequestTimeLoggerImpl) dispatch(request Request) {
	bucket := hash(request.identifier) % uint32(tl.concurrentLogsCount)
	tl.loggerRoutines[bucket].ProcessRequest(request)
}

type TimeLogger struct {
	volatileRecords map[string]Record
	file            *os.File
}

func NewTimeLogger(filePath string, logIdentifier string) *TimeLogger {
	return &TimeLogger{
		volatileRecords: make(map[string]Record),
		file:            createFile(path.Join(filePath, logIdentifier+".log")),
	}
}

func (tl *TimeLogger) processRequest(request Request) {
	if request.requestType == START {
		tl.volatileRecords[request.identifier] = Record{
			identifier:       request.identifier,
			startRequestTime: request.timestamp,
		}
	} else if request.requestType == END {
		record, ok := tl.volatileRecords[request.identifier]
		if !ok {
			log.Printf("Could not find the start request for '%v'\n", request.identifier)
			return
		}
		(&record).endRequestTime = request.timestamp
		err := writeRecordToFile(tl.file, record)
		if err != nil {
			log.Printf("Could not log end timestamp for request '%v': %v\n", request.identifier, err)
			return
		}
		delete(tl.volatileRecords, request.identifier)

	} else {
		log.Panicf("Request type %v is malformed: it needs to be either START or END", request.requestType)
	}
}

func (tl *TimeLogger) close() {
	err := tl.file.Close()
	if err != nil {
		log.Printf("Could not close the file '%v': %v", tl.file.Name(), err)
	}
}

type TimeLoggerRoutine struct {
	timeLogger *TimeLogger
	ch         chan Request
	wg         *sync.WaitGroup
}

func NewTimeLoggerRoutine(timeLogger *TimeLogger, wg *sync.WaitGroup) *TimeLoggerRoutine {
	return &TimeLoggerRoutine{timeLogger: timeLogger, ch: make(chan Request, 1000), wg: wg}
}

func (tr *TimeLoggerRoutine) Start() {
	for request := range tr.ch {
		tr.timeLogger.processRequest(request)
	}
	tr.timeLogger.close()
	tr.wg.Done()
}

func (tr *TimeLoggerRoutine) ProcessRequest(r Request) {
	tr.ch <- r
}

func (tr *TimeLoggerRoutine) Close() {
	close(tr.ch)
}

type requestType string

const START requestType = "START"
const END requestType = "END"

type Request struct {
	identifier  string
	requestType requestType
	timestamp   time.Time
}

type Record struct {
	identifier       string
	startRequestTime time.Time
	endRequestTime   time.Time
}

func writeRecordToFile(f *os.File, record Record) error {
	startTimeStr := strconv.FormatInt(record.startRequestTime.UnixMilli(), 10)
	endTimeStr := strconv.FormatInt(record.endRequestTime.UnixMilli(), 10)
	deltaStr := strconv.FormatInt(record.endRequestTime.Sub(record.startRequestTime).Milliseconds(), 10)
	return appendLineToFile(f, record.identifier+","+startTimeStr+","+endTimeStr+","+deltaStr)
}
func createFile(filePath string) *os.File {
	f, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}
	return f
}
func appendLineToFile(f *os.File, content string) error {
	_, err := f.Write([]byte(content + "\n"))
	return err
}

func hash(s string) uint32 {
	return crc32.ChecksumIEEE([]byte(s))
}
