package model

import "strconv"

type TransactionRequest struct {
	TransactionId   string
	SourceIban      string
	DestinationIban string
	Amount          int
}

func NewTransactionRequest(srcId string, dstId string, amount int) TransactionRequest {
	return TransactionRequest{
		TransactionId:   "TX" + srcId + "->" + dstId + ":" + strconv.Itoa(amount),
		SourceIban:      srcId,
		DestinationIban: dstId,
		Amount:          amount,
	}
}

type TransactionResponse struct {
	TransactionId string
	Success       bool
}

type Account struct {
	Iban   string
	Amount int
}
