package sut

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"log"
	"main/dynamoutils"
	"main/utils"
	"main/worker/domain"
	"strconv"
	"sync"
	"time"
)

func HotelReservationLoadState(parameters *HotelReservationParameters, client *dynamodb.Client) error {

	newActors := make(map[domain.ActorId]any)
	var newEntities []utils.Pair[domain.CollectionId, domain.QueryableItem]

	for partitionIndex := range parameters.HotelsPartitionsCount {

		for shardIndex := range parameters.HotelsShardsPerPartitionCount {
			for hotelIndex := range parameters.HotelsPerShardCount {
				hotelId := buildHotelId(partitionIndex, shardIndex, hotelIndex)
				hotel, weekAvailabilities := buildHotel(hotelId, parameters.WeeksCount, parameters.RoomsPerTypeCount)
				newActors[hotel.GetId()] = hotel

				for _, weekAvailability := range weekAvailabilities {
					newEntities = append(newEntities, utils.Pair[domain.CollectionId, domain.QueryableItem]{First: domain.CollectionId{Id: hotel.GetId().String() + "/WeekAvailabilities", TypeName: "WeekAvailability"}, Second: weekAvailability})
				}
			}
		}
	}

	characters := "abcdefghijklmnopqrstuvwxyz"

	for i := range characters {
		partitionName := string(characters[i])
		for shardIndex := range parameters.UserShardsPerPartitionCount {
			shardName := strconv.Itoa(shardIndex)
			for userIndex := range parameters.UsersPerShardCount {
				userId := domain.ActorId{
					InstanceId: strconv.Itoa(userIndex),
					PhyPartitionId: domain.PhysicalPartitionId{
						PartitionName:         partitionName,
						PhysicalPartitionName: shardName,
					},
				}
				user := domain.NewUser()
				user.SetId(userId)
				newActors[user.GetId()] = user
			}
		}
	}

	err := dynamoutils.AddActorStateBatch(client, newActors)

	if err != nil {
		return err
	}

	return dynamoutils.AddEntityBatch(client, newEntities)
}

func HotelReservationLoadInboxesAndTasks(parameters *HotelReservationParameters, client *dynamodb.Client) error {

	newMessages, newTasks := HotelReservationBuildInboxesAndTasks(parameters)
	err := dynamoutils.AddMessageBatch(client, newMessages)
	if err != nil {
		return err
	}

	return dynamoutils.AddActorTaskBatch(client, newTasks)

}

func SlowlyLoadInboxes(
	newMessages []utils.Pair[domain.ActorMessage, domain.ActorId], client *dynamodb.Client,
	sendingPeriod time.Duration, maxRequestsPerPeriod int, initialDelay time.Duration) error {

	requestQueue := make(chan utils.Pair[domain.ActorMessage, domain.ActorId], maxRequestsPerPeriod)
	var wg sync.WaitGroup
	for range maxRequestsPerPeriod {
		go func() {
			for request := range requestQueue {
				localErr := dynamoutils.AddMessage(client, request.First, request.Second)
				if localErr != nil {
					log.Printf("Could not add the message %v: %v\n", request.First.Id, localErr)
				}
				wg.Done()
			}
		}()
	}

	time.Sleep(initialDelay)
	i := 0
	for {
		endExcludedIndex := i + maxRequestsPerPeriod
		if endExcludedIndex > len(newMessages) {
			endExcludedIndex = len(newMessages)
		}

		messageBatch := newMessages[i:endExcludedIndex]
		wg.Add(len(messageBatch))

		for _, message := range messageBatch {
			requestQueue <- message
		}

		time.Sleep(sendingPeriod)
		wg.Wait()

		i = endExcludedIndex

		if i >= len(newMessages) {
			break
		}
	}

	close(requestQueue)

	return nil
}

func HotelReservationBuildInboxesAndTasks(parameters *HotelReservationParameters) ([]utils.Pair[domain.ActorMessage, domain.ActorId], []domain.PhysicalPartitionId) {
	var newMessages []utils.Pair[domain.ActorMessage, domain.ActorId]
	newTasks := utils.NewMapSet[domain.PhysicalPartitionId]()

	characters := "abcdefghijklmnopqrstuvwxyz"

	dstHotelPartitionIndex := 0
	dstHotelShardIndex := 0
	dstHotelIndex := 0

	userInteractionSeed := 0

	for partitionIndex := range parameters.ActiveUserPartitionsCount {
		partitionName := string(characters[partitionIndex])
		for shardIndex := range parameters.ActiveUserShardsCount {
			shardName := strconv.Itoa(shardIndex)

			for userIndex := range parameters.ActiveUsersPerShardCount {
				userId := domain.ActorId{
					InstanceId: strconv.Itoa(userIndex),
					PhyPartitionId: domain.PhysicalPartitionId{
						PartitionName:         partitionName,
						PhysicalPartitionName: shardName,
					},
				}
				newTasks.Add(userId.PhyPartitionId)

				for j := range parameters.RequestsPerUser {
					roomType := domain.STANDARD
					if j%2 == 0 {
						roomType = domain.PREMIUM
					}

					message := domain.ActorMessage{
						Id: domain.MessageIdentifier{
							ActorId:         userId,
							UniqueTimestamp: "",
						},
						SenderId: domain.ActorId{
							InstanceId:     "-",
							PhyPartitionId: domain.PhysicalPartitionId{PartitionName: "", PhysicalPartitionName: ""},
						},
						Content: domain.BookingRequest{
							UserId:   userId,
							HotelId:  buildHotelId(dstHotelPartitionIndex, dstHotelShardIndex, dstHotelIndex),
							RoomType: roomType,
							BookingPeriod: domain.BookingPeriod{
								Week:      strconv.Itoa(userInteractionSeed % parameters.ActiveWeeksCount),
								DayOfWeek: userInteractionSeed % 7,
							},
						},
					}

					newMessages = append(newMessages, utils.Pair[domain.ActorMessage, domain.ActorId]{First: message, Second: userId})
					dstHotelIndex++

					if dstHotelIndex == parameters.ActiveHotelsPerShardCount {
						dstHotelIndex = 0
						dstHotelShardIndex++
					}
					if dstHotelShardIndex == parameters.ActiveHotelShardsPerPartitionCount {
						dstHotelShardIndex = 0
						dstHotelPartitionIndex++
					}
					if dstHotelPartitionIndex == parameters.ActiveHotelPartitionsCount {
						dstHotelPartitionIndex = 0
					}

					userInteractionSeed++
				}
			}
		}

	}

	return newMessages, newTasks.ToSlice()

}

func PopulateHotelReservationScenario(parameters *HotelReservationParameters, client *dynamodb.Client) error {
	err := HotelReservationLoadState(parameters, client)
	if err != nil {
		return err
	}

	err = HotelReservationLoadInboxesAndTasks(parameters, client)
	return err

}

func buildHotel(id domain.ActorId, weeksCount int, roomsPerTypeCount int) (*domain.Hotel, []*domain.WeekAvailability) {
	var weekAvailabilities []*domain.WeekAvailability
	hotel := domain.NewHotel(id)

	availableRooms := make(map[int]map[domain.RoomType]map[string]struct{})
	roomTypes := []domain.RoomType{domain.STANDARD, domain.PREMIUM}

	for day := range 7 {
		availableRooms[day] = make(map[domain.RoomType]map[string]struct{})
		for _, roomType := range roomTypes {
			availableRooms[day][roomType] = make(map[string]struct{})
			for roomNumber := range roomsPerTypeCount {
				initial := string(roomType[0])
				roomId := initial + "ROOM" + strconv.Itoa(roomNumber)
				availableRooms[day][roomType][roomId] = struct{}{}
			}
		}
	}

	for week := range weeksCount {
		weekAvailabilities = append(weekAvailabilities, domain.NewWeekAvailability(
			strconv.Itoa(week),
			availableRooms,
		))
	}

	return hotel, weekAvailabilities

}

func buildHotelId(partitionIndex int, shardIndex int, hotelIndex int) domain.ActorId {
	return domain.ActorId{
		InstanceId: strconv.Itoa(hotelIndex),
		PhyPartitionId: domain.PhysicalPartitionId{
			PartitionName:         "City" + strconv.Itoa(partitionIndex),
			PhysicalPartitionName: strconv.Itoa(shardIndex),
		},
	}
}

type HotelReservationParameters struct {
	HotelsPartitionsCount         int
	HotelsShardsPerPartitionCount int
	HotelsPerShardCount           int
	WeeksCount                    int
	RoomsPerTypeCount             int
	UserShardsPerPartitionCount   int
	UsersPerShardCount            int

	ActiveHotelPartitionsCount         int
	ActiveHotelShardsPerPartitionCount int
	ActiveHotelsPerShardCount          int
	ActiveWeeksCount                   int
	ActiveUserPartitionsCount          int
	ActiveUserShardsCount              int
	ActiveUsersPerShardCount           int
	RequestsPerUser                    int
}

type SlowLoadingParams struct {
	SendingPeriodMillis  int64
	MaxRequestsPerPeriod int
	InitialDelayMillis   int64
}
