package baseline

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"main/baseline/hotel-reservation/model"
	"main/dynamoutils"
	"strconv"
)

func LoadBaselineHotelReservationState(params BaselineHotelReservationParameters, client *dynamodb.Client) error {
	var hotels []dynamoutils.BaselineHotel
	var userIds []string

	for i := range params.HotelsCount {
		hotels = append(hotels, buildHotel("Hotel/"+strconv.Itoa(i), params.WeeksCount, params.RoomsPerTypeCount))
	}
	for i := range params.UsersCount {
		userIds = append(userIds, "User/"+strconv.Itoa(i))
	}

	err := dynamoutils.AddBaselineHotelsBatch(client, hotels)
	if err != nil {
		return err
	}

	return dynamoutils.AddBaselineHotelUsersBatch(client, userIds)
}

func buildHotel(hotelId string, weeksCount int, roomsPerTypeCount int) dynamoutils.BaselineHotel {
	var weekAvailabilities []model.WeekAvailability

	availableRooms := make(map[int]map[model.RoomType]map[string]struct{})
	roomTypes := []model.RoomType{model.STANDARD, model.PREMIUM}

	for day := range 7 {
		availableRooms[day] = make(map[model.RoomType]map[string]struct{})
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
		weekAvailabilities = append(weekAvailabilities, *model.NewWeekAvailability(
			strconv.Itoa(week),
			availableRooms,
		))
	}

	return dynamoutils.BaselineHotel{
		Id:                  hotelId,
		WeeksAvailabilities: weekAvailabilities,
	}
}

type BaselineHotelReservationParameters struct {
	HotelsCount       int
	WeeksCount        int
	RoomsPerTypeCount int
	UsersCount        int
}
