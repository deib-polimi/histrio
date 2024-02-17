package sut

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/dynamoutils"
	"main/utils"
	"main/worker/domain"
	"strconv"
)

func BankingLoadState(parameters *BankingParameters, client *dynamodb.Client) error {

	newActors := make(map[domain.ActorId]any)
	var newEntities []utils.Pair[domain.CollectionId, domain.QueryableItem]

	for bankPartitionIndex := range parameters.BankPartitionsCount {
		for bankShardIndex := range parameters.BankShardsPerPartitionCount {
			for bankIndex := range parameters.BanksPerShardCount {
				bankId := buildBankId(bankPartitionIndex, bankShardIndex, bankIndex)
				bankActor := domain.NewBankBranch(bankId)

				newActors[bankActor.GetId()] = bankActor

				newActors[bankActor.GetId()] = bankActor
				for accountId := range parameters.AccountsPerBankCount {
					account := domain.Account{Id: strconv.Itoa(accountId), Amount: 10_000}
					newEntities = append(newEntities, utils.Pair[domain.CollectionId, domain.QueryableItem]{First: domain.CollectionId{Id: bankActor.GetId().String() + "/Accounts", TypeName: "Account"}, Second: &account})
				}
			}
		}
	}

	err := dynamoutils.AddActorStateBatch(client, newActors)

	if err != nil {
		return err
	}

	return dynamoutils.AddEntityBatch(client, newEntities)
}

func BankingLoadInboxesAndTasks(parameters *BankingParameters, client *dynamodb.Client) error {
	newMessages, newTasks := BankingBuildInboxesAndTasks(parameters)
	err := dynamoutils.AddMessageBatch(client, newMessages)
	if err != nil {
		return err
	}

	return dynamoutils.AddActorTaskBatch(client, newTasks)
}

func BankingBuildInboxesAndTasks(parameters *BankingParameters) ([]utils.Pair[domain.ActorMessage, domain.ActorId], []domain.PhysicalPartitionId) {
	var newMessages []utils.Pair[domain.ActorMessage, domain.ActorId]
	newTasks := utils.NewMapSet[domain.PhysicalPartitionId]()

	seed := 0
	for partitionIndex := range parameters.ActiveBankPartitionsCount {
		for bankShardIndex := range parameters.ActiveBankShardsPerPartitionCount {
			for bankIndex := range parameters.ActiveBanksPerShardCount {
				bankId := buildBankId(partitionIndex, bankShardIndex, bankIndex)
				newTasks.Add(bankId.PhyPartitionId)
				for accountIndex := range parameters.ActiveAccountsPerBankCount {
					for transactionIndexPerAccount := range parameters.TransactionsPerAccountCount {
						destinationIndex := (accountIndex + transactionIndexPerAccount) % parameters.ActiveAccountsPerBankCount
						if destinationIndex == accountIndex {
							destinationIndex = (destinationIndex + 1) % parameters.ActiveBankPartitionsCount
						}
						transactionAmount := seed % 1000
						seed++
						transactionRequest := domain.NewTransactionRequest(strconv.Itoa(accountIndex), strconv.Itoa(destinationIndex), transactionAmount)
						actorMessage := domain.ActorMessage{
							Id: domain.MessageIdentifier{
								ActorId:         bankId,
								UniqueTimestamp: "",
							},
							SenderId: domain.ActorId{
								InstanceId:     "-",
								PhyPartitionId: domain.PhysicalPartitionId{PartitionName: "", PhysicalPartitionName: ""},
							},
							Content: transactionRequest,
						}
						newMessages = append(newMessages, utils.Pair[domain.ActorMessage, domain.ActorId]{First: actorMessage, Second: bankId})
					}
				}
			}
		}
	}

	return newMessages, newTasks.ToSlice()
}

func buildBankId(bankPartitionIndex int, bankShardIndex int, bankIndex int) domain.ActorId {
	return domain.ActorId{
		InstanceId: strconv.Itoa(bankIndex),
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "Bank" + strconv.Itoa(bankPartitionIndex),
			PhysicalPartitionName: strconv.Itoa(bankShardIndex),
		},
	}
}

type BankingParameters struct {
	BankPartitionsCount         int
	BankShardsPerPartitionCount int
	BanksPerShardCount          int
	AccountsPerBankCount        int

	ActiveBankPartitionsCount         int
	ActiveBankShardsPerPartitionCount int
	ActiveBanksPerShardCount          int
	ActiveAccountsPerBankCount        int
	TransactionsPerAccountCount       int
}
