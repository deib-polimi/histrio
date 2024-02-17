package main

import (
	"bytes"
	"context"
	"encoding/json"
	"github.com/aws/aws-lambda-go/lambda"
	"io"
	"log"
	"net/http"
	"time"
)

func init() {

}

func handler(_ context.Context, evt json.RawMessage) error {
	var url string
	err := json.Unmarshal(evt, &url)

	if err != nil {
		return err
	}

	startTime := time.Now()
	r, err := http.NewRequest("POST", url, bytes.NewBuffer([]byte(`{
		"title": "Post title",
		"body": "Post description",
		"userId": 1
	}`)))

	if err != nil {
		return err
	}

	client := &http.Client{}
	res, err := client.Do(r)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		myErr := Body.Close()
		if myErr != nil {
			panic(myErr)
		}
	}(res.Body)

	endTime := time.Since(startTime)
	bodyBytes, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}
	log.Printf("Response from ClockMaster: %v\n", string(bodyBytes))
	log.Printf("Delta milliseconds: %v", endTime.Milliseconds())

	return nil
}

func main() {
	lambda.Start(handler)
}
