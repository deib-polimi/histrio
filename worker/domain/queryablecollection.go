package domain

import (
	"log"
	"main/utils"
	"reflect"
)

type QueryableCollection[T QueryableItem] struct {
	CollectionId CollectionId

	context      ExecutionContext
	fetchedItems map[string]T
	dirtyItems   utils.Set[string]
}

func (qc *QueryableCollection[T]) Init(context ExecutionContext) {
	qc.context = context
	qc.fetchedItems = make(map[string]T)
	qc.context.actorManager.AddItemCollector(qc)
	if qc.CollectionId.Id == "" {
		qc.CollectionId.Id = context.entityId + "/" + context.fieldName
	}
	typeName, err := context.entityLoader.GetNameByType(reflect.TypeFor[T]().Elem())
	if err != nil {
		log.Fatalf("FATAL: There is not any name registered for type '%v'", reflect.TypeFor[T]().Elem().String())
	}
	qc.CollectionId.TypeName = typeName
	qc.dirtyItems = utils.NewMapSet[string]()
}

func (qc *QueryableCollection[T]) GetCollectionId() CollectionId {
	return qc.CollectionId
}

func (qc *QueryableCollection[T]) GetDirtyItems() map[string]QueryableItem {
	result := make(map[string]QueryableItem)
	for _, id := range qc.dirtyItems.ToSlice() {
		result[id] = qc.fetchedItems[id]
	}

	qc.dirtyItems.Clear()

	return result
}

func (qc *QueryableCollection[T]) CommitDirtyItems() {
	qc.dirtyItems.Clear()
}

func (qc *QueryableCollection[T]) DropDirtyItems() {
	for _, dirtyItem := range qc.dirtyItems.ToSlice() {
		delete(qc.fetchedItems, dirtyItem)
	}

	qc.dirtyItems.Clear()
}

func (qc *QueryableCollection[T]) Get(itemId string) (T, error) {
	cachedItem, ok := qc.fetchedItems[itemId]
	if ok == false {
		myType := reflect.TypeFor[T]().Elem()
		anyItem, err := qc.context.queryableCollectionDao.GetItem(qc.CollectionId, myType, itemId, qc.context)
		cachedItem = anyItem.(T)
		if err != nil {
			return *new(T), err
		}
		qc.fetchedItems[itemId] = cachedItem
	}

	qc.dirtyItems.Add(itemId)

	return cachedItem, nil
}

func (qc *QueryableCollection[T]) Find(attribute string, value string) ([]T, error) {
	var queryResult []T
	entities, err := qc.context.queryableCollectionDao.FindItems(
		qc.CollectionId,
		reflect.TypeFor[T](),
		attribute,
		value,
		qc.context,
	)

	if err != nil {
		return queryResult, err
	}

	for _, entity := range entities {
		typedEntity := entity.(T)
		qc.fetchedItems[typedEntity.GetId()] = typedEntity
		qc.dirtyItems.Add(typedEntity.GetId())
		queryResult = append(queryResult, typedEntity)
	}

	return queryResult, nil
}
