package common

type Cat struct {
	Name string
	Tags []*TagValue
}

type TagValue struct {
	Name  string
	Value interface{}
}

type Tag struct {
	Name   string
	Values []interface{}
}
