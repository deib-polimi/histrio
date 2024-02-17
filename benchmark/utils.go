package benchmark

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"log"
	"main/utils"
	"main/worker/domain"
	"strconv"
	"time"
)

type MetricsExporter interface {
	Export(records [][]string) error
}

func ComputeAndExportBenchmarkResults(client *dynamodb.Client, runId string, longMetricsExporter MetricsExporter, shortMetricsExporter MetricsExporter) error {
	var lastKey map[string]types.AttributeValue
	var correlationInfos []domain.CorrelationInfo
	for {
		result, err := client.Query(context.TODO(), &dynamodb.QueryInput{
			TableName: aws.String("Outbox"),
			ExpressionAttributeValues: map[string]types.AttributeValue{
				":runId": &types.AttributeValueMemberS{Value: runId},
			},
			KeyConditionExpression: aws.String("run_id = :runId"),
			ExclusiveStartKey:      lastKey,
		})

		if err != nil {
			log.Fatal(err)
		}
		lastKey = result.LastEvaluatedKey

		for _, item := range result.Items {
			bookingResponseJson := item["content"].(*types.AttributeValueMemberS).Value
			bookingResponse := domain.BookingResponse{}
			err = json.Unmarshal([]byte(bookingResponseJson), &bookingResponse)
			if err != nil {
				log.Fatal(err)
			}
			correlationInfos = append(correlationInfos, bookingResponse.CorrelationInfo)
		}

		if len(result.LastEvaluatedKey) == 0 {
			break
		}
	}

	records := [][]string{
		{"id", "fromUserToHotelMillis", "fromHotelToUserMillis", "totalTime"},
	}

	//extract metrics
	var averageResponseTime float64
	responseTimesProcessedSoFar := 0
	maximumTimestamp := time.Time{}
	minimumTimestamp := time.Now()

	for _, info := range correlationInfos {
		averageResponseTime = (averageResponseTime*float64(responseTimesProcessedSoFar) + float64(info.Samples[2].EndTime-info.Samples[0].StartTime)) / float64(responseTimesProcessedSoFar+1)
		responseTimesProcessedSoFar++
		if time.UnixMilli(info.Samples[0].StartTime).Before(minimumTimestamp) {
			minimumTimestamp = time.UnixMilli(info.Samples[0].StartTime)
		}
		if time.UnixMilli(info.Samples[2].EndTime).After(maximumTimestamp) {
			maximumTimestamp = time.UnixMilli(info.Samples[2].EndTime)
		}
		bSample := NewHotelBenchmarkSample(info)
		records = append(records, []string{
			bSample.id,
			strconv.Itoa(int(bSample.fromUserToHotel.Milliseconds())),
			strconv.Itoa(int(bSample.fromHotelToUser.Milliseconds())),
			strconv.Itoa(int(bSample.totalTime.Milliseconds())),
		})
	}
	err := longMetricsExporter.Export(records)

	if err != nil {
		return err
	}

	throughput := float64(len(correlationInfos)) / float64(maximumTimestamp.Sub(minimumTimestamp).Milliseconds())
	err = shortMetricsExporter.Export([][]string{
		{"averageResponseTime", "throughput"},
		{strconv.FormatFloat(averageResponseTime, 'f', 3, 64), strconv.FormatFloat(throughput, 'f', 3, 64)},
	})

	if err != nil {
		return err
	}

	return nil
}

type HotelBenchmarkSample struct {
	id              string
	fromUserToHotel time.Duration
	fromHotelToUser time.Duration
	totalTime       time.Duration
}

func NewHotelBenchmarkSample(requestInfo domain.CorrelationInfo) *HotelBenchmarkSample {
	return &HotelBenchmarkSample{
		id:              requestInfo.Id,
		fromUserToHotel: time.Duration(requestInfo.Samples[1].StartTime-requestInfo.Samples[0].EndTime) * time.Millisecond,
		fromHotelToUser: time.Duration(requestInfo.Samples[2].StartTime-requestInfo.Samples[1].EndTime) * time.Millisecond,
		totalTime:       time.Duration(requestInfo.Samples[2].EndTime-requestInfo.Samples[0].StartTime) * time.Millisecond,
	}
}

type CsvExporter struct {
	name string
}

func NewCsvExporter(name string) *CsvExporter {
	return &CsvExporter{name: name}
}

func (l *CsvExporter) Export(records [][]string) error {
	return utils.ExportToCsv(l.name, records)
}

type LogExporter struct {
	name string
}

func NewLogExporter(name string) *LogExporter {
	return &LogExporter{name: name}
}

func (l *LogExporter) Export(records [][]string) error {
	for rowIndex := 1; rowIndex < len(records); rowIndex++ {
		entry := ""
		entry += fmt.Sprintf("table: %v, row: %v, {", l.name, rowIndex)
		sep := ""
		for colIndex := 0; colIndex < len(records[rowIndex]); colIndex++ {
			entry += sep
			entry += fmt.Sprintf("%v: %v", records[0][colIndex], records[rowIndex][colIndex])
			sep = ", "
		}
		entry += fmt.Sprintf("}")
		log.Println(entry)
	}

	return nil
}
