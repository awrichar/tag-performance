package mongotest

import (
	"context"
	"log"
	"time"

	"dev/tagperformance/internal/common"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func runMongoQuery(ctx context.Context, name string, db *mongo.Collection, query bson.M, queryLimit int64) (time.Duration, error) {
	start := time.Now()
	log.Print(query)
	rows, err := db.Find(ctx, query, &options.FindOptions{Limit: &queryLimit})
	if err != nil {
		return 0, err
	}
	count := 0
	for rows.Next(ctx) {
		count++
	}
	log.Printf("%d rows", count)
	duration := time.Since(start)
	log.Printf("%s took %v", name, duration)
	return duration, nil
}

func SetupMongo(ctx context.Context, db *mongo.Client, cats []*common.Cat) error {
	log.Print("Building mongo")
	coll := db.Database("cats").Collection("cats")
	coll.DeleteMany(ctx, bson.D{})
	coll.Indexes().DropAll(ctx)
	if _, err := coll.Indexes().CreateOne(ctx, mongo.IndexModel{
		Keys: bson.M{"tags": 1},
	}); err != nil {
		return err
	}
	counter := 0
	batchMax := 1000
	batch := make([]interface{}, 0, batchMax)
	for _, cat := range cats {
		counter++
		common.PrintCounter(counter)
		catTags := make(map[string]interface{}, len(cat.Tags))
		for _, tag := range cat.Tags {
			catTags[tag.Name] = tag.Value
		}
		batch = append(batch, bson.M{
			"name": cat.Name,
			"tags": catTags,
		})
		if len(batch) >= batchMax {
			if _, err := coll.InsertMany(ctx, batch); err != nil {
				return err
			}
			batch = make([]interface{}, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if _, err := coll.InsertMany(ctx, batch); err != nil {
			return err
		}
	}
	return nil
}

func QueryMongo(ctx context.Context, db *mongo.Client, queryLimit int64) ([]time.Duration, error) {
	coll := db.Database("cats").Collection("cats")
	d1, err := runMongoQuery(ctx, "mongo (1 tag)", coll, bson.M{
		"tags.color": "brown",
	}, queryLimit)
	if err != nil {
		return nil, err
	}

	d2, err := runMongoQuery(ctx, "mongo (3 tags)", coll, bson.M{
		"tags.color":    "brown",
		"tags.age":      bson.M{"$gte": 10},
		"tags.demeanor": "grumpy",
	}, queryLimit)
	if err != nil {
		return nil, err
	}
	return []time.Duration{d1, d2}, nil
}
