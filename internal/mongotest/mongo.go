package mongotest

import (
	"context"
	"log"
	"time"

	"dev/tagperformance/internal/common"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func runMongoQuery(ctx context.Context, name string, db *mongo.Collection, query bson.M) error {
	start := time.Now()
	defer func() {
		log.Printf("%s took %v", name, time.Since(start))
	}()
	log.Print(query)
	count, err := db.CountDocuments(ctx, query)
	if err != nil {
		return err
	}
	log.Printf("%d rows", count)
	return nil
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
		catTags := make(map[string]string, len(cat.Tags))
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

func QueryMongo(ctx context.Context, db *mongo.Client) error {
	coll := db.Database("cats").Collection("cats")
	if err := runMongoQuery(ctx, "mongo (1 tag)", coll, bson.M{
		"tags.color": "brown",
	}); err != nil {
		return err
	}
	if err := runMongoQuery(ctx, "mongo (3 tags)", coll, bson.M{
		"tags.color":    "brown",
		"tags.age":      bson.M{"$gte": "4"},
		"tags.demeanor": "grumpy",
	}); err != nil {
		return err
	}
	return nil
}
