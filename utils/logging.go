package utils

import (
	"encoding/csv"
	"log"
	"os"
	"path/filepath"
	"runtime"
)

var (
	_, b, _, _ = runtime.Caller(0)

	// Root folder of this project
	Root = filepath.Join(filepath.Dir(b), "")
)

func SetLogger(fileName string) error {
	file, err := openLogFile(filepath.Join(filepath.Dir(Root), "log", fileName+".txt"))
	if err != nil {
		return err
	}
	log.SetOutput(file)
	log.SetFlags(log.LstdFlags | log.Lshortfile | log.Lmicroseconds)

	log.Println("log file created")
	return nil
}

func openLogFile(path string) (*os.File, error) {
	logFile, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	return logFile, nil
}

func ExportToCsv(name string, records [][]string) error {
	file, err := os.Create(filepath.Join(filepath.Dir(Root), "log", name+".csv"))
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range records {
		if err = writer.Write(value); err != nil {
			return err
		}
	}

	return err
}
