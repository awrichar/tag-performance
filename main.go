package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"strings"
	"time"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

const doSetup = true
const maxCats = 2000000
const maxTags = 20

const postgresUrl = "postgresql://postgres@localhost:8000/perf?sslmode=disable"
const mongoUrl = "mongodb://localhost:8001"

type Cat struct {
	name string
	tags []string
}

func makeTags() []string {
	n := rand.Intn(10) + 1
	tags := make([]string, 0, n)
	used := make(map[string]bool, n)
	for i := 0; i < n; i++ {
		tag := fmt.Sprint(rand.Intn(maxTags))
		if !used[tag] {
			tags = append(tags, tag)
			used[tag] = true
		}
	}
	return tags
}

func runSQLQuery(name string, query sq.SelectBuilder) error {
	start := time.Now()
	defer func() {
		log.Printf("%s took %v", name, time.Since(start))
	}()
	sql, _, _ := query.ToSql()
	log.Print(sql)
	rows, err := query.Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	var count interface{}
	if rows.Next() {
		if err := rows.Scan(&count); err != nil {
			return err
		}
		log.Printf("%v rows", count)
	}
	return nil
}

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

func printCounter(counter int) {
	if counter%10000 == 0 {
		fmt.Printf("%d ", counter)
		if counter%100000 == 0 {
			fmt.Println("")
		}
	}
}

func insertBatchJoinTable(tx *sql.Tx, batch []*Cat) error {
	inserts := make([]string, 0)
	for _, c := range batch {
		inserts = append(inserts, fmt.Sprintf("('%s')", c.name))
	}
	result, err := tx.Query(fmt.Sprintf("INSERT INTO cats(name) VALUES %s RETURNING id", strings.Join(inserts, ",")))
	if err != nil {
		return err
	}
	i := 0
	vals := make([]string, 0)
	for result.Next() {
		var id int
		result.Scan(&id)
		for _, tag := range batch[i].tags {
			vals = append(vals, fmt.Sprintf("(%d,%s)", id, tag))
		}
		i++
	}
	_, err = tx.Exec("INSERT INTO cat_tags(cat_id, tag_id) VALUES " + strings.Join(vals, ","))
	return err
}

func setupJoinTable(db *sql.DB, cats []*Cat) error {
	log.Print("Building join table")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
        DROP TABLE IF EXISTS cats;
        DROP TABLE IF EXISTS cat_tags;
        DROP INDEX IF EXISTS cat_tags_x;
        CREATE TABLE cats(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
        CREATE TABLE cat_tags(cat_id INTEGER NOT NULL, tag_id INTEGER NOT NULL);
        CREATE INDEX cats_x ON cats(id);
        CREATE INDEX cat_tags_x ON cat_tags(tag_id);
    `)
	if err != nil {
		return err
	}
	counter := 0
	batchMax := 100
	batch := make([]*Cat, 0, batchMax)
	for _, cat := range cats {
		counter++
		printCounter(counter)
		batch = append(batch, cat)
		if len(batch) >= batchMax {
			if err := insertBatchJoinTable(tx, batch); err != nil {
				return err
			}
			batch = make([]*Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchJoinTable(tx, batch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func queryJoinTable(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats").
		Join("cat_tags tag1 ON cats.id = tag1.cat_id").
		Where("tag1.tag_id = 2").
		RunWith(db)
	if err := runSQLQuery("join table (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats").
		Join("cat_tags tag1 ON cats.id = tag1.cat_id").
		Join("cat_tags tag2 ON cats.id = tag2.cat_id").
		Join("cat_tags tag3 ON cats.id = tag3.cat_id").
		Join("cat_tags tag4 ON cats.id = tag4.cat_id").
		Where("tag1.tag_id = 2").
		Where("tag2.tag_id = 5").
		Where("tag3.tag_id = 7").
		Where("tag4.tag_id = 8").
		RunWith(db)
	if err := runSQLQuery("join table (4 tags)", query); err != nil {
		return err
	}
	return nil
}

func insertBatchArrayColumn(tx *sql.Tx, batch []*Cat) error {
	inserts := make([]string, 0)
	for _, c := range batch {
		tags := "{" + strings.Join(c.tags, ",") + "}"
		inserts = append(inserts, fmt.Sprintf("('%s', '%s')", c.name, tags))
	}
	_, err := tx.Exec("INSERT INTO cats_array(name, tags) VALUES " + strings.Join(inserts, ","))
	return err
}

func setupArrayColumn(db *sql.DB, cats []*Cat) error {
	log.Print("Building array column")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
        DROP TABLE IF EXISTS cats_array;
        DROP INDEX IF EXISTS cats_array_x;
        CREATE TABLE cats_array(name VARCHAR NOT NULL, tags INTEGER[]);
        CREATE INDEX cats_array_x ON cats_array USING GIN(tags);
    `)
	if err != nil {
		return err
	}
	counter := 0
	batchMax := 100
	batch := make([]*Cat, 0, batchMax)
	for _, cat := range cats {
		counter++
		printCounter(counter)
		batch = append(batch, cat)
		if len(batch) >= batchMax {
			if err := insertBatchArrayColumn(tx, batch); err != nil {
				return err
			}
			batch = make([]*Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchArrayColumn(tx, batch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func queryArrayColumn(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats_array").
		Where(sq.Expr("tags @> '{2}'")).
		RunWith(db)
	if err := runSQLQuery("array column (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats_array").
		Where(sq.Expr("tags @> '{2,5,7,8}'")).
		RunWith(db)
	if err := runSQLQuery("array column (4 tags)", query); err != nil {
		return err
	}
	return nil
}

func setupMongo(ctx context.Context, db *mongo.Client, cats []*Cat) error {
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
		printCounter(counter)
		batch = append(batch, bson.M{
			"name": cat.name,
			"tags": cat.tags,
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

func queryMongo(ctx context.Context, db *mongo.Client) error {
	coll := db.Database("cats").Collection("cats")
	if err := runMongoQuery(ctx, "mongo (1 tag)", coll, bson.M{
		"tags": "2",
	}); err != nil {
		return err
	}
	if err := runMongoQuery(ctx, "mongo (4 tags)", coll, bson.M{
		"tags": bson.M{
			"$all": bson.A{"2", "5", "7", "8"},
		},
	}); err != nil {
		return err
	}
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
		log.Printf("Building tables using %d cats with up to %d tags each", maxCats, maxTags)
		cats := make([]*Cat, maxCats)
		for i := range cats {
			cats[i] = &Cat{
				name: fmt.Sprintf("cat-%d", i),
				tags: makeTags(),
			}
		}
		if err := setupJoinTable(psql, cats); err != nil {
			log.Fatal(err)
		}
		if err := setupArrayColumn(psql, cats); err != nil {
			log.Fatal(err)
		}
		if err := setupMongo(ctx, mdb, cats); err != nil {
			log.Fatal(err)
		}
		log.Print("Done building tables")
	}

	if err := queryJoinTable(psql); err != nil {
		log.Fatal(err)
	}
	if err := queryArrayColumn(psql); err != nil {
		log.Fatal(err)
	}
	if err := queryMongo(ctx, mdb); err != nil {
		log.Fatal(err)
	}
}
