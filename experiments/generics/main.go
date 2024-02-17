package main

import (
	"fmt"
	"reflect"
)

type Identifiable interface {
	GetId() string
}

type IdentifiableList[T Identifiable] struct {
	myList []T
}

func (i *IdentifiableList[T]) getAll() []Identifiable {
	var tmp []Identifiable
	for _, elem := range i.myList {
		tmp = append(tmp, elem)
	}
	return tmp
}

func (i *IdentifiableList[T]) Add(elems ...T) {
	for _, elem := range elems {
		i.myList = append(i.myList, elem)
	}
}

func main() {
	var iAnimalList IdentifiableList[*Animal]
	iAnimalList.Add(&Animal{"A0"})
	iAnimalList.Add(&Animal{"A1"})

	var iBuildingList IdentifiableList[*Building]
	iBuildingList.Add(&Building{"B0"})
	iBuildingList.Add(&Building{"B1"})

	var iList IdentifiableList[Identifiable]
	iList.Add(iAnimalList.getAll()...)
	iList.Add(iBuildingList.getAll()...)

	/*for _, elem := range iList.getAll() {
		fmt.Println(elem.GetId())
	}
	*/

	list := reflect.ValueOf(iAnimalList)
	field := list.Type().Field(0).Name
	fmt.Println(field)
}

type Animal struct {
	id string
}

func (a *Animal) GetId() string {
	return a.id
}

type Building struct {
	id string
}

func (b *Building) GetId() string {
	return b.id
}
