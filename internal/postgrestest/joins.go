package postgrestest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

func insertBatchJoinTable(tx *sql.Tx, batch []*common.Cat, tagMap map[string]int) error {
	inserts := make([]string, len(batch))
	args := make([]interface{}, len(batch))
	for i, cat := range batch {
		args[i] = cat.Name
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
		for _, tag := range batch[i].Tags {
			args = append(args, id)
			args = append(args, tagMap[tag.Name])
			args = append(args, tag.Value)
			vals = append(vals, fmt.Sprintf("($%d, $%d, $%d)", len(args)-2, len(args)-1, len(args)))
		}
	}
	_, err = tx.Exec("INSERT INTO cat_tags(cat_id, tag_id, value) VALUES "+strings.Join(vals, ","), args...)
	return err
}

func SetupJoinTable(db *sql.DB, cats []*common.Cat, tags []*common.Tag) error {
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
		CREATE TABLE tags(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
        CREATE TABLE cat_tags(cat_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, value VARCHAR NOT NULL);
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
	batch := make([]*common.Cat, 0, batchMax)
	for _, cat := range cats {
		counter++
		common.PrintCounter(counter)
		batch = append(batch, cat)
		if len(batch) >= batchMax {
			if err := insertBatchJoinTable(tx, batch, tagMap); err != nil {
				return err
			}
			batch = make([]*common.Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchJoinTable(tx, batch, tagMap); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func QueryJoinTable(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats").
		Join("cat_tags tag1 ON cats.id = tag1.cat_id").
		Join("tags tagn1 ON tag1.tag_id = tagn1.id").
		Where(sq.Eq{
			"tagn1.name": "color",
			"tag1.value": "brown",
		}).
		PlaceholderFormat(sq.Dollar).
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
		Where(sq.And{
			sq.Eq{
				"tagn1.name": "color",
				"tag1.value": "brown",
				"tagn2.name": "age",
				"tagn3.name": "demeanor",
				"tag3.value": "grumpy",
			},
			sq.GtOrEq{
				"tag2.value": "4",
			},
		}).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("join table (3 tags)", query); err != nil {
		return err
	}
	return nil
}
