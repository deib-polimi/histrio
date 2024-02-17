package utils

import (
	"errors"
	"testing"
	"time"
)

func tenErrorsThenAllSuccessGenerator() func() (struct{}, error) {
	i := 0
	return func() (struct{}, error) {
		if i < 10 {
			i++
			return struct{}{}, errors.New("fake error")
		} else {
			return struct{}{}, nil
		}
	}
}

func TestRetrier(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping test in short mode.")
	}
	myRetrier := NewRetrier[struct{}](NewExponentialBackoffStrategy(
		-1,
		50*time.Millisecond,
		0.1,
		2*time.Second,
	))

	myFunc := tenErrorsThenAllSuccessGenerator()
	for range 50 {
		_, err := myRetrier.DoWithReturn(myFunc)
		if err != nil {
			t.Fatal(err)
		}
	}
}
