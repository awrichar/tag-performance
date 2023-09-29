package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"

	"dev/tagperformance/internal/common"
	"dev/tagperformance/internal/mongotest"
	"dev/tagperformance/internal/postgrestest"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const doSetup = true
const maxCats = 2000000

const postgresUrl = "postgresql://postgres@localhost:8000/perf?sslmode=disable"
const mongoUrl = "mongodb://localhost:8001"

func makeTags() []*common.Tag {
	return []*common.Tag{
		{Name: "color", Values: []interface{}{"brown", "orange", "black"}},
		{Name: "age", Values: []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		{Name: "demeanor", Values: []interface{}{"friendly", "grumpy"}},
	}
}

func makeCats(tags []*common.Tag) []*common.Cat {
	cats := make([]*common.Cat, maxCats)
	for i := range cats {
		cats[i] = &common.Cat{
			Name: fmt.Sprintf("cat-%d", i),
			Tags: chooseTags(tags),
		}
	}
	return cats
}

func chooseTags(tags []*common.Tag) []*common.TagValue {
	chosen := make([]*common.TagValue, 0)
	for _, tag := range tags {
		choice := rand.Intn(len(tag.Values) + 1)
		if choice > 0 {
			chosen = append(chosen, &common.TagValue{
				Name:  tag.Name,
				Value: tag.Values[choice-1],
			})
		}
	}
	return chosen
}

func main() {
	ctx := context.Background()

	psql, err := sql.Open("postgres", postgresUrl)
	if err != nil {
		log.Fatal(err)
	}

	mdb, err := mongo.Connect(ctx, options.Client().
		ApplyURI(mongoUrl).
		SetServerAPIOptions(options.ServerAPI(options.ServerAPIVersion1)))
	if err != nil {
		log.Fatal(err)
	}

	if doSetup {
		tags := makeTags()
		cats := makeCats(tags)
		log.Printf("Building tables using %d cats with up to %d tags each", len(cats), len(tags))
		if err := postgrestest.SetupJoinTable(psql, cats, tags); err != nil {
			log.Fatal(err)
		}
		if err := postgrestest.SetupArrayColumn(psql, cats, tags); err != nil {
			log.Fatal(err)
		}
		if err := postgrestest.SetupJSONColumn(psql, cats, tags); err != nil {
			log.Fatal(err)
		}
		if err := mongotest.SetupMongo(ctx, mdb, cats); err != nil {
			log.Fatal(err)
		}
		log.Print("Done building tables")
	}

	if err := postgrestest.QueryJoinTable(psql); err != nil {
		log.Fatal(err)
	}
	if err := postgrestest.QueryArrayColumn(psql); err != nil {
		log.Fatal(err)
	}
	if err := postgrestest.QueryJSONColumn(psql); err != nil {
		log.Fatal(err)
	}
	if err := mongotest.QueryMongo(ctx, mdb); err != nil {
		log.Fatal(err)
	}
}
