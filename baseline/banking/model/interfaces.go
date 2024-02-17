package model

type AccountDao interface {
	GetAndLockAccount(iban string) (Account, error)
	UnlockAccount(iban string) error
	UpdateAccount(account Account) error
}
