package plugins

import (
	"bytes"
	"io"
	"log"
	"main/worker/domain"
	"net/http"
	"net/url"
)

type TimestampCollectorFactoryLocalImpl struct {
}

func NewTimestampCollectorFactoryLocalImpl() *TimestampCollectorFactoryLocalImpl {
	return &TimestampCollectorFactoryLocalImpl{}
}

func (tcFactory *TimestampCollectorFactoryLocalImpl) BuildTimestampCollector() domain.TimestampCollector {
	return &TimestampCollectorLocalImpl{}
}

type TimestampCollectorFactoryImpl struct {
	client  *http.Client
	baseUrl string
}

func NewTimestampCollectorFactoryImpl(client *http.Client, baseUrl string) *TimestampCollectorFactoryImpl {
	return &TimestampCollectorFactoryImpl{client: client, baseUrl: baseUrl}
}

func (tcFactory *TimestampCollectorFactoryImpl) BuildTimestampCollector() domain.TimestampCollector {
	return NewTimestampCollectorImpl(tcFactory.client, tcFactory.baseUrl)
}

type TimestampCollectorImpl struct {
	client  *http.Client
	baseUrl string
}

func NewTimestampCollectorImpl(client *http.Client, baseUrl string) *TimestampCollectorImpl {
	return &TimestampCollectorImpl{client: client, baseUrl: baseUrl}
}

func (tc *TimestampCollectorImpl) StartMeasurement(identifier string) error {
	myUrl := tc.baseUrl + "/start/" + url.QueryEscape(identifier)
	r, err := http.NewRequest("POST", myUrl, bytes.NewBuffer([]byte(`{}`)))
	if err != nil {
		return err
	}
	res, err := tc.client.Do(r)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		myErr := Body.Close()
		if myErr != nil {
			panic(myErr)
		}
	}(res.Body)

	return nil
}

func (tc *TimestampCollectorImpl) EndMeasurement(identifier string) error {
	myUrl := tc.baseUrl + "/end/" + url.QueryEscape(identifier)
	r, err := http.NewRequest("POST", myUrl, bytes.NewBuffer([]byte(`{}`)))
	if err != nil {
		return err
	}
	res, err := tc.client.Do(r)
	if err != nil {
		return err
	}
	defer func(Body io.ReadCloser) {
		myErr := Body.Close()
		if myErr != nil {
			panic(myErr)
		}
	}(res.Body)
	bodyBytes, err := io.ReadAll(res.Body)
	log.Printf("Request with id %v ended in %v ns\n", identifier, string(bodyBytes))

	return nil
}

type TimestampCollectorLocalImpl struct{}

func (tc *TimestampCollectorLocalImpl) StartMeasurement(identifier string) error {
	log.Printf("Start measurement for %v\n", identifier)

	return nil
}
func (tc *TimestampCollectorLocalImpl) EndMeasurement(identifier string) error {
	log.Printf("End measurement for %v\n", identifier)
	return nil
}
