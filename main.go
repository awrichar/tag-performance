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

const postgresUrl = "postgresql://postgres@localhost:8000/perf?sslmode=disable"
const mongoUrl = "mongodb://localhost:8001"

type Cat struct {
	name string
	tags []string
}

type Tag struct {
	name  string
	color string
}

func makeTags() []*Tag {
	return []*Tag{
		{name: "friendly", color: "blue"},
		{name: "color:brown", color: "brown"},
		{name: "color:orange", color: "orange"},
		{name: "color:black", color: "black"},
		{name: "age:3", color: "red"},
		{name: "age:4", color: "red"},
		{name: "age:5", color: "red"},
		{name: "age:6", color: "red"},
	}
}

func makeCats(tags []*Tag) []*Cat {
	cats := make([]*Cat, maxCats)
	for i := range cats {
		cats[i] = &Cat{
			name: fmt.Sprintf("cat-%d", i),
			tags: chooseTags(tags),
		}
	}
	return cats
}

func chooseTags(tags []*Tag) []string {
	chosen := make([]string, 0)
	for _, tag := range tags {
		if rand.Intn(2) == 1 {
			chosen = append(chosen, tag.name)
		}
	}
	return chosen
}

func runSQLQuery(name string, query sq.SelectBuilder) error {
	start := time.Now()
	defer func() {
		log.Printf("%s took %v", name, time.Since(start))
	}()
	sql, args, _ := query.ToSql()
	log.Printf("%s; args:%v", sql, args)
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

func insertBatchJoinTable(tx *sql.Tx, batch []*Cat, tagMap map[string]int) error {
	inserts := make([]string, len(batch))
	args := make([]interface{}, len(batch))
	for i, cat := range batch {
		args[i] = cat.name
		inserts[i] = fmt.Sprintf("($%d)", i+1)
	}
	result, err := tx.Query(fmt.Sprintf("INSERT INTO cats(name) VALUES %s RETURNING id", strings.Join(inserts, ",")), args...)
	if err != nil {
		return err
	}
	vals := make([]string, 0, len(inserts))
	args = make([]interface{}, 0, len(inserts)*2)
	for i := 0; result.Next(); i++ {
		var id int
		if err := result.Scan(&id); err != nil {
			return err
		}
		for _, tag := range batch[i].tags {
			args = append(args, id)
			args = append(args, tagMap[tag])
			vals = append(vals, fmt.Sprintf("($%d, $%d)", len(args)-1, len(args)))
		}
	}
	_, err = tx.Exec("INSERT INTO cat_tags(cat_id, tag_id) VALUES "+strings.Join(vals, ","), args...)
	return err
}

func buildTagMap(tx *sql.Tx, tags []*Tag) (map[string]int, error) {
	tagMap := make(map[string]int, len(tags))
	for _, tag := range tags {
		result, err := tx.Query("INSERT INTO tags(name, color) VALUES($1, $2) RETURNING id, name", tag.name, tag.color)
		if err != nil {
			return nil, err
		}
		for result.Next() {
			var id int
			var name string
			if err := result.Scan(&id, &name); err != nil {
				return nil, err
			}
			tagMap[name] = id
		}
	}
	return tagMap, nil
}

func setupJoinTable(db *sql.DB, cats []*Cat, tags []*Tag) error {
	log.Print("Building join table")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
        DROP TABLE IF EXISTS cats;
		DROP TABLE IF EXISTS tags;
        DROP TABLE IF EXISTS cat_tags;
        DROP INDEX IF EXISTS cat_tags_x;
        CREATE TABLE cats(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
		CREATE TABLE tags(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, color VARCHAR NOT NULL);
        CREATE TABLE cat_tags(cat_id INTEGER NOT NULL, tag_id INTEGER NOT NULL);
        CREATE INDEX cats_x ON cats(id);
		CREATE INDEX tags_x ON tags(id);
        CREATE INDEX cat_tags_x ON cat_tags(tag_id);
    `)
	if err != nil {
		return err
	}
	tagMap, err := buildTagMap(tx, tags)
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
			if err := insertBatchJoinTable(tx, batch, tagMap); err != nil {
				return err
			}
			batch = make([]*Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchJoinTable(tx, batch, tagMap); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func queryJoinTable(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats").
		Join("cat_tags tag1 ON cats.id = tag1.cat_id").
		Join("tags ON tag1.tag_id = tags.id").
		Where("tags.name = $1", "friendly").
		RunWith(db)
	if err := runSQLQuery("join table (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats").
		Join("cat_tags tag1 ON cats.id = tag1.cat_id").
		Join("cat_tags tag2 ON cats.id = tag2.cat_id").
		Join("cat_tags tag3 ON cats.id = tag3.cat_id").
		Join("tags tagn1 ON tag1.tag_id = tagn1.id").
		Join("tags tagn2 ON tag2.tag_id = tagn2.id").
		Join("tags tagn3 ON tag3.tag_id = tagn3.id").
		Where("tagn1.name = $1", "friendly").
		Where("tagn2.name = $2", "color:brown").
		Where("tagn3.name = $3", "age:4").
		RunWith(db)
	if err := runSQLQuery("join table (3 tags)", query); err != nil {
		return err
	}
	return nil
}

func insertBatchArrayColumn(tx *sql.Tx, batch []*Cat, tagMap map[string]int) error {
	inserts := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch))
	for i, cat := range batch {
		tags := make([]string, len(cat.tags))
		for j, tag := range cat.tags {
			tags[j] = fmt.Sprint(tagMap[tag])
		}
		args = append(args, cat.name)
		args = append(args, "{"+strings.Join(tags, ",")+"}")
		inserts[i] = fmt.Sprintf("($%d, $%d)", len(args)-1, len(args))
	}
	_, err := tx.Exec("INSERT INTO cats_array(name, tags) VALUES "+strings.Join(inserts, ","), args...)
	return err
}

func setupArrayColumn(db *sql.DB, cats []*Cat, tags []*Tag) error {
	log.Print("Building array column")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
        DROP TABLE IF EXISTS cats_array;
		DROP TABLE IF EXISTS tags;
        DROP INDEX IF EXISTS cats_array_x;
        CREATE TABLE cats_array(name VARCHAR NOT NULL, tags INTEGER[]);
		CREATE TABLE tags(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL, color VARCHAR NOT NULL);
        CREATE INDEX cats_array_x ON cats_array USING GIN(tags);
    `)
	if err != nil {
		return err
	}
	tagMap, err := buildTagMap(tx, tags)
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
			if err := insertBatchArrayColumn(tx, batch, tagMap); err != nil {
				return err
			}
			batch = make([]*Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchArrayColumn(tx, batch, tagMap); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func queryArrayColumn(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats_array").
		Where(sq.Expr("tags @> ARRAY(SELECT id FROM tags WHERE name = $1)", "friendly")).
		RunWith(db)
	if err := runSQLQuery("array column (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats_array").
		Where(sq.Expr("tags @> ARRAY(SELECT id FROM tags WHERE name IN ($1, $2, $3))", "friendly", "color:brown", "age:4")).
		RunWith(db)
	if err := runSQLQuery("array column (3 tags)", query); err != nil {
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
		"tags": "friendly",
	}); err != nil {
		return err
	}
	if err := runMongoQuery(ctx, "mongo (3 tags)", coll, bson.M{
		"tags": bson.M{
			"$all": bson.A{"friendly", "color:brown", "age:4"},
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
		tags := makeTags()
		cats := makeCats(tags)
		log.Printf("Building tables using %d cats with up to %d tags each", len(cats), len(tags))
		if err := setupJoinTable(psql, cats, tags); err != nil {
			log.Fatal(err)
		}
		if err := setupArrayColumn(psql, cats, tags); err != nil {
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
