package worker_simulation

import (
	"io"
	"log"
	"net/http"
	"time"
)

type Manager struct {
	channel                chan bool
	hasPreviousTransaction bool
	client                 *http.Client
}

func (m *Manager) ConsumeMessage() {
	m.hasPreviousTransaction = true
	go handleTransaction(m.channel, m.client)
}

func (m *Manager) ProcessPreviousTransaction() {
	success := <-m.channel
	if success == false {
		log.Fatal("False value in channel")
	}
	m.hasPreviousTransaction = false
}

func handleTransaction(channel chan bool, client *http.Client) {
	startTime := time.Now()

	req, err := http.NewRequest("GET", "http://localhost:8001", nil)
	if err != nil {
		log.Fatal(err)
	}

	req.Close = true

	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Could not send request: %v\n", err)
		channel <- true
		return
	}

	_, err2 := io.ReadAll(resp.Body)

	if err2 != nil {
		log.Fatal(err2)
	}

	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			log.Fatal(err)
		}
	}(resp.Body)

	log.Printf("Transaction delay: %v", time.Since(startTime))
	channel <- true
}

func createClient() *http.Client {
	t := http.DefaultTransport.(*http.Transport).Clone()
	//t.DisableKeepAlives = true
	client := &http.Client{Transport: t}
	return client
}

func WorkerMain(managersCount int, messagesToProcess int) {
	var managers []Manager

	client := createClient()

	for range managersCount {
		managers = append(managers, Manager{channel: make(chan bool), client: client})
	}
	processedMessagesCount := 0

	for {
		for i := range managersCount {
			manager := &managers[i]
			if manager.hasPreviousTransaction {
				manager.ProcessPreviousTransaction()
				processedMessagesCount++
			}
			manager.ConsumeMessage()
		}

		if processedMessagesCount == messagesToProcess {
			break
		}
	}

}
