package domain

import (
	"errors"
	"reflect"
	"strings"
)
import "encoding/json"

type EntityLoader struct {
	registry        map[string]reflect.Type
	inverseRegistry map[reflect.Type]string
}

func NewEntityLoader() *EntityLoader {
	return &EntityLoader{
		registry:        make(map[string]reflect.Type),
		inverseRegistry: make(map[reflect.Type]string),
	}
}

func (r *EntityLoader) RegisterType(typeName string, newType reflect.Type) {
	r.registry[typeName] = newType
	r.inverseRegistry[newType] = typeName
}

func (r *EntityLoader) GetTypeByName(typeName string) (reflect.Type, error) {
	myType, ok := r.registry[typeName]
	if ok == false {
		return reflect.TypeOf(0), errors.New("cannot find a registered type for type name '" + typeName + "'")
	}

	return myType, nil
}

func (r *EntityLoader) GetNameByType(myType reflect.Type) (string, error) {
	myTypeName, ok := r.inverseRegistry[myType]
	if ok == false {
		return "", errors.New("cannot find a registered type name for type '" + myType.String() + "'")
	}

	return myTypeName, nil
}

func (r *EntityLoader) LoadEntityByTypeName(serializedState string, targetTypeName string, context ExecutionContext) (any, error) {
	targetType, err := r.GetTypeByName(targetTypeName)
	if err != nil {
		return 0, err
	}
	return r.LoadEntity(serializedState, targetType, context)
}
func (r *EntityLoader) LoadEntity(serializedState string, targetType reflect.Type, context ExecutionContext) (any, error) {
	item := reflect.New(targetType).Interface()
	err := json.Unmarshal([]byte(serializedState), item)
	if err != nil {
		return item, err
	}

	e := reflect.Indirect(reflect.ValueOf(item))

	for i := range e.NumField() {
		fieldType := e.Field(i).Type()
		if strings.Contains(fieldType.String(), "QueryableCollection") {
			subContext := context
			subContext.fieldName = e.Type().Field(i).Name
			e.Field(i).Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(subContext)})
		} else if strings.Contains(fieldType.String(), "ActorSpawner") {
			subContext := context
			subContext.fieldName = e.Type().Field(i).Name
			e.Field(i).Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(subContext)})
		} else if strings.Contains(fieldType.String(), "MessageSender") {
			subContext := context
			subContext.fieldName = e.Type().Field(i).Name
			e.Field(i).Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(subContext)})
		} else if strings.Contains(fieldType.String(), "BenchmarkHelper") {
			subContext := context
			subContext.fieldName = e.Type().Field(i).Name
			e.Field(i).Addr().MethodByName("Init").Call([]reflect.Value{reflect.ValueOf(subContext)})
		}
	}

	return item, nil
}
