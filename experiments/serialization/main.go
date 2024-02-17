package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
)

type DirtyItemCollector interface {
	GetDirtyItems() map[string]QueryableItem
	GetCollectionId() string
}

type QueryableItem interface {
	GetId() string
	GetQueryableAttributes() map[string]string
	GetQueryableAttributeValue(string) string
}

type QueryableCollection[T QueryableItem] struct {
	CollectionId string

	context      MyContext
	fetchedItems map[string]T
}

func (qc *QueryableCollection[T]) Init(context MyContext) {
	qc.context = context
	qc.fetchedItems = make(map[string]T)
	qc.context.ActorManager.AddItemCollectors(qc)
}

func (qc *QueryableCollection[T]) GetCollectionId() string {
	return qc.CollectionId
}

func (qc *QueryableCollection[T]) GetDirtyItems() map[string]QueryableItem {
	result := make(map[string]QueryableItem)
	for id := range qc.fetchedItems {
		result[id] = qc.fetchedItems[id]
	}

	return result
}

func (qc *QueryableCollection[T]) Get(itemId string) T {
	cachedItem, ok := qc.fetchedItems[itemId]
	if ok == false {
		var err error
		myType := reflect.TypeFor[T]()
		fmt.Println(myType.String())
		desItem, err := deserializeQueryableItem(qc.context.myDb[itemId], reflect.TypeFor[T]().Elem(), qc.context)
		cachedItem = desItem.(T)
		if err != nil {
			log.Fatal(err)
		}
		qc.fetchedItems[itemId] = cachedItem
	}

	return cachedItem
}

type ActorManager struct {
	dirtyItemCollectors []DirtyItemCollector
}

func (am *ActorManager) AddItemCollectors(collectors ...DirtyItemCollector) {
	for _, collector := range collectors {
		am.dirtyItemCollectors = append(am.dirtyItemCollectors, collector)
	}
}

func (am *ActorManager) Collect() map[string]map[string]string {
	result := make(map[string]map[string]string)
	for _, collector := range am.dirtyItemCollectors {
		collectorMap := make(map[string]string)
		for _, item := range collector.GetDirtyItems() {
			serializedState, err := json.Marshal(item)
			if err != nil {
				log.Fatal(err)
			}
			collectorMap[item.GetId()] = string(serializedState)
		}
		result[collector.GetCollectionId()] = collectorMap
	}

	return result
}

type MyContext struct {
	ActorManager *ActorManager
	myDb         map[string]string
}

func deserializeQueryableItem(serializedState string, targetType reflect.Type, context MyContext) (any, error) {
	item := reflect.New(targetType).Interface()
	err := json.Unmarshal([]byte(serializedState), item)
	if err != nil {
		return item, err
	}

	e := reflect.Indirect(reflect.ValueOf(item))

	for i := range e.NumField() {
		fieldType := e.Field(i).Type()
		if strings.Contains(fieldType.String(), "QueryableCollection") {
			e.Field(i).Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(context)})
		}
	}

	return item, nil
}

func getZeroInstance(targetType string) (any, error) {
	switch targetType {
	case "User":
		return &User{}, nil
	case "Reservation":
		return &Reservation{}, nil
	default:
		return &struct{}{}, errors.New("Type '" + targetType + "' is not registered")
	}
}

func main() {

	userToSerialize := User{Id: "Pippo", Reservations: QueryableCollection[*Reservation]{
		CollectionId: "PippoReservations",
	}}

	serializedUser, err := json.Marshal(userToSerialize)
	if err != nil {
		log.Fatal(err)
	}

	jsonSerializedUser := string(serializedUser)

	serializedReservation, err := json.Marshal(Reservation{Id: "R001", RoomCount: 2})
	if err != nil {
		log.Fatal(err)
	}

	jsonSerializedReservation := string(serializedReservation)

	actorManager := ActorManager{}
	myDb := make(map[string]string)
	myDb["R001"] = jsonSerializedReservation

	deserializedUser, err := deserializeQueryableItem(jsonSerializedUser, reflect.TypeOf(userToSerialize), MyContext{ActorManager: &actorManager, myDb: myDb})
	if err != nil {
		log.Fatal(err)
	}
	typedDeserializedUser := deserializedUser.(*User)
	typedDeserializedUser.GetReservation("R001")
	collectedItems := actorManager.Collect()
	for collectorId, items := range collectedItems {
		fmt.Printf("%v:\n", collectorId)
		for itemId, item := range items {
			fmt.Printf("%v -> %v\n", itemId, item)
		}
	}
}

type User struct {
	Id           string
	Reservations QueryableCollection[*Reservation]
}

func (u *User) GetId() string {
	return u.Id
}

func (u *User) SetId(id string) {
	u.Id = id
}

func (u *User) GetReservation(id string) *Reservation {
	return u.Reservations.Get(id)
}

func (u *User) GetQueryableAttributes() map[string]string {
	return make(map[string]string)

}

func (u *User) GetQueryableAttributeValue(string) string {
	return ""
}

type Reservation struct {
	Id        string
	RoomCount int
}

func (r *Reservation) GetId() string {
	return r.Id
}

func (r *Reservation) FilterEquals(attributeName string, value string) bool {
	if attributeName == "RoomCount" {
		return strconv.Itoa(r.RoomCount) == value
	} else {
		return false
	}
}

func (r *Reservation) SetRoomCount(count int) {
	r.RoomCount = count
}

func (r *Reservation) GetQueryableAttributes() map[string]string {
	return make(map[string]string)
}
func (r *Reservation) GetQueryableAttributeValue(attr string) string {
	return ""
}
