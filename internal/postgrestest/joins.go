package postgrestest

import (
	"context"
	"database/sql"
	"log"
	"time"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
)

const initJoinTables = `
DROP TABLE IF EXISTS cats_j;
DROP TABLE IF EXISTS tags_j;
DROP TABLE IF EXISTS cat_tags_j;
DROP INDEX IF EXISTS tags_j_name;
DROP INDEX IF EXISTS cat_tags_j_val;
CREATE TABLE cats_j(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE tags_j(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE cat_tags_j(cat_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, value VARCHAR NOT NULL);
CREATE UNIQUE INDEX tags_j_name ON tags_j(name);
CREATE INDEX cat_tags_j_val ON cat_tags_j(tag_id, value, cat_id);
`

func insertBatchJoinTable(tx *sql.Tx, batch []*common.Cat, tagMap map[string]int) error {
	ins := sq.Insert("cats_j").Columns("name").PlaceholderFormat(sq.Dollar)
	for _, cat := range batch {
		ins = ins.Values(cat.Name)
	}
	rows, err := ins.Suffix("RETURNING id").RunWith(tx).Query()
	if err != nil {
		return err
	}
	defer rows.Close()
	ins = sq.Insert("cat_tags_j").Columns("cat_id", "tag_id", "value").PlaceholderFormat(sq.Dollar)
	for i := 0; rows.Next(); i++ {
		var id int
		if err := rows.Scan(&id); err != nil {
			return err
		}
		for _, tag := range batch[i].Tags {
			ins = ins.Values(id, tagMap[tag.Name], tag.Value)
		}
	}
	_, err = ins.RunWith(tx).Exec()
	return err
}

func SetupJoinTable(db *sql.DB, cats []*common.Cat, tags []*common.Tag) error {
	log.Print("Building join table")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(initJoinTables)
	if err != nil {
		return err
	}
	tagMap, err := buildTagMap(tx, tags, "tags_j")
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

func QueryJoinTable(db *sql.DB, queryLimit uint64) ([]time.Duration, error) {
	query := sq.Select("cats_j.name").From("cats_j, cat_tags_j tag1").
		Where(sq.And{
			sq.Expr("tag1.cat_id = cats_j.id"),
			sq.Expr("tag1.tag_id = (?)", sq.Select("id").From("tags_j").Where(sq.Eq{"name": "color"}).Limit(1)),
			sq.Eq{"tag1.value": "brown"},
		}).
		Limit(queryLimit).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	d1, err := runSQLQuery("join table (1 tag)", query)
	if err != nil {
		return nil, err
	}

	query = sq.Select("cats_j.name").From("cats_j, cat_tags_j tag1, cat_tags_j tag2, cat_tags_j tag3").
		Where(sq.And{
			sq.Expr("tag1.cat_id = cats_j.id"),
			sq.Expr("tag2.cat_id = cats_j.id"),
			sq.Expr("tag3.cat_id = cats_j.id"),
			sq.Expr("tag1.tag_id = (?)", sq.Select("id").From("tags_j").Where(sq.Eq{"name": "color"}).Limit(1)),
			sq.Expr("tag2.tag_id = (?)", sq.Select("id").From("tags_j").Where(sq.Eq{"name": "age"}).Limit(1)),
			sq.Expr("tag3.tag_id = (?)", sq.Select("id").From("tags_j").Where(sq.Eq{"name": "demeanor"}).Limit(1)),
			sq.Eq{"tag1.value": "brown"},
			sq.GtOrEq{"tag2.value": 10},
			sq.Eq{"tag3.value": "grumpy"},
		}).
		Limit(queryLimit).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	d2, err := runSQLQuery("join table (3 tags)", query)
	if err != nil {
		return nil, err
	}
	return []time.Duration{d1, d2}, nil
}
