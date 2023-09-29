package common

type Cat struct {
	Name string
	Tags []*TagValue
}

type TagValue struct {
	Name  string
	Value string
}

type Tag struct {
	Name   string
	Values []string
}
