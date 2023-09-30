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
const queryLimit = 1000
const generateTags = 10

const postgresUrl = "postgresql://postgres@localhost:8000/perf?sslmode=disable"
const mongoUrl = "mongodb://localhost:8001"

func makeTags() []*common.Tag {
	tags := []*common.Tag{
		{Name: "color", Values: []interface{}{"brown", "orange", "black"}},
		{Name: "age", Values: []interface{}{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15}},
		{Name: "demeanor", Values: []interface{}{"friendly", "grumpy"}},
	}
	vals := make([]interface{}, 100)
	for i := 0; i < len(vals); i++ {
		vals[i] = i
	}
	for i := 0; i < generateTags; i++ {
		tags = append(tags, &common.Tag{
			Name:   fmt.Sprintf("tag-%d", i+1),
			Values: vals,
		})
	}
	return tags
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

func runQueries(ctx context.Context, psql *sql.DB, mdb *mongo.Client) error {
	join_d, err := postgrestest.QueryJoinTable(psql, queryLimit)
	if err != nil {
		return err
	}
	array_d, err := postgrestest.QueryArrayColumn(psql, queryLimit)
	if err != nil {
		return err
	}
	json_d, err := postgrestest.QueryJSONColumn(psql, queryLimit)
	if err != nil {
		return err
	}
	mongo_d, err := mongotest.QueryMongo(ctx, mdb, queryLimit)
	if err != nil {
		return err
	}
	fmt.Printf(`
	PostgreSQL (join table): %v
	PostgreSQL (array column): %v
	PostgreSQL (jsonb column): %v
	MongoDB: %v`+"\n", join_d, array_d, json_d, mongo_d)
	return nil
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

	if err := runQueries(ctx, psql, mdb); err != nil {
		log.Fatal(err)
	}
}
