package services

import (
	"log"
	"main/baseline/banking/model"
	"main/utils"
)

type BankingService struct {
	accountDao model.AccountDao
}

func NewBankingService(accountDao model.AccountDao) *BankingService {
	return &BankingService{accountDao: accountDao}
}

func (bs *BankingService) ExecuteTransaction(transactionRequest model.TransactionRequest) (model.TransactionResponse, error) {
	retrier := utils.NewDefaultRetrier[model.TransactionResponse]()

	return retrier.DoWithReturn(func() (model.TransactionResponse, error) {
		sourceAccount, err := bs.accountDao.GetAndLockAccount(transactionRequest.SourceIban)
		if err != nil {
			return model.TransactionResponse{}, err
		}

		defer func() {
			sourceUnlockRetrier := utils.NewDefaultRetrier[struct{}]()
			_, unlockErr := sourceUnlockRetrier.DoWithReturn(func() (struct{}, error) {
				return struct{}{}, bs.accountDao.UnlockAccount(transactionRequest.SourceIban)
			})

			if unlockErr != nil {
				log.Fatalf("Could not unlock account %v : %v", transactionRequest.SourceIban, unlockErr)
			}
		}()

		destinationAccount, err := bs.accountDao.GetAndLockAccount(transactionRequest.DestinationIban)

		if err != nil {
			return model.TransactionResponse{}, err
		}

		defer func() {
			destinationUnlockRetrier := utils.NewDefaultRetrier[struct{}]()
			_, unlockErr := destinationUnlockRetrier.DoWithReturn(func() (struct{}, error) {
				return struct{}{}, bs.accountDao.UnlockAccount(transactionRequest.DestinationIban)
			})
			if unlockErr != nil {
				log.Fatalf("Could not unlock account %v : %v", transactionRequest.DestinationIban, unlockErr)
			}
		}()

		if sourceAccount.Amount-transactionRequest.Amount < 0 {
			return model.TransactionResponse{
				TransactionId: transactionRequest.TransactionId,
				Success:       false,
			}, nil
		}

		destinationAccount.Amount += transactionRequest.Amount
		sourceAccount.Amount -= transactionRequest.Amount

		err = bs.accountDao.UpdateAccount(sourceAccount)
		if err != nil {
			log.Printf("Failed to update account %v: %v", sourceAccount.Iban, err)
		}

		err = bs.accountDao.UpdateAccount(destinationAccount)
		if err != nil {
			log.Printf("Failed to update account %v: %v", destinationAccount.Iban, err)
		}

		return model.TransactionResponse{
			TransactionId: transactionRequest.TransactionId,
			Success:       true,
		}, nil

	})
}
