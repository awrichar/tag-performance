package postgrestest

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"time"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
)

const initArrayTables = `
DROP TABLE IF EXISTS cats_a;
DROP TABLE IF EXISTS tags_a;
DROP TABLE IF EXISTS tag_values_a;
DROP INDEX IF EXISTS cats_a_tags;
DROP INDEX IF EXISTS tags_a_name;
DROP INDEX IF EXISTS tag_values_a_name;
CREATE TABLE cats_a(name VARCHAR NOT NULL, tags INTEGER[]);
CREATE TABLE tags_a(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE tag_values_a(id SERIAL PRIMARY KEY, tag_id INTEGER NOT NULL, value VARCHAR NOT NULL);
CREATE INDEX cats_a_tags ON cats_a USING GIN(tags);
CREATE UNIQUE INDEX tags_a_name ON tags_a(name);
CREATE UNIQUE INDEX tag_values_a_name ON tag_values_a(tag_id, value);
`

func insertBatchArrayColumn(tx *sql.Tx, batch []*common.Cat, tagValueMap map[string]int) error {
	ins := sq.Insert("cats_a").Columns("name", "tags").PlaceholderFormat(sq.Dollar)
	for _, cat := range batch {
		catTags := make([]interface{}, len(cat.Tags))
		var placeholders string
		if len(cat.Tags) > 0 {
			for j, tag := range cat.Tags {
				catTags[j] = tagValueMap[fmt.Sprintf("%s:%s", tag.Name, tag.Value)]
			}
			placeholders = strings.Repeat(",?", len(catTags))[1:]
		}
		ins = ins.Values(cat.Name, sq.Expr("ARRAY["+placeholders+"]::integer[]", catTags...))
	}
	_, err := ins.RunWith(tx).Exec()
	return err
}

func buildTagValueMap(tx *sql.Tx, tags []*common.Tag, tagMap map[string]int) (map[string]int, error) {
	tagValueMap := make(map[string]int, 0)
	ins := sq.Insert("tag_values_a").Columns("tag_id", "value").PlaceholderFormat(sq.Dollar)
	for _, tag := range tags {
		for _, val := range tag.Values {
			ins = ins.Values(tagMap[tag.Name], val)
		}
	}
	rows, err := ins.Suffix("RETURNING id").RunWith(tx).Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for _, tag := range tags {
		for _, val := range tag.Values {
			if !rows.Next() {
				break
			}
			var valID int
			if err := rows.Scan(&valID); err != nil {
				return nil, err
			}
			tagValueMap[fmt.Sprintf("%s:%s", tag.Name, val)] = valID
		}
	}
	return tagValueMap, nil
}

func SetupArrayColumn(db *sql.DB, cats []*common.Cat, tags []*common.Tag) error {
	log.Print("Building array column")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(initArrayTables)
	if err != nil {
		return err
	}
	tagMap, err := buildTagMap(tx, tags, "tags_a")
	if err != nil {
		return err
	}
	tagValueMap, err := buildTagValueMap(tx, tags, tagMap)
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
			if err := insertBatchArrayColumn(tx, batch, tagValueMap); err != nil {
				return err
			}
			batch = make([]*common.Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchArrayColumn(tx, batch, tagValueMap); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func QueryArrayColumn(db *sql.DB, queryLimit uint64) ([]time.Duration, error) {
	query := sq.Select("cats_a.name").From("cats_a").
		Where(
			sq.Expr("cats_a.tags && ARRAY(?)",
				sq.Select("tag_values_a.id").From("tag_values_a").
					Join("tags_a ON tag_values_a.tag_id = tags_a.id").
					Where(sq.Eq{
						"tags_a.name":        "color",
						"tag_values_a.value": "brown",
					}),
			),
		).
		Limit(queryLimit).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	d1, err := runSQLQuery("array column (1 tag)", query)
	if err != nil {
		return nil, err
	}

	query = sq.Select("cats_a.name").From("cats_a").
		Where(sq.Expr("cats_a.tags @> ARRAY(?)",
			sq.Select("tag_values_a.id").From("tag_values_a").
				Join("tags_a ON tag_values_a.tag_id = tags_a.id").
				Where(sq.Or{
					sq.Eq{
						"tags_a.name":        "color",
						"tag_values_a.value": "brown",
					},
					sq.Eq{
						"tags_a.name":        "demeanor",
						"tag_values_a.value": "grumpy",
					},
				}),
		)).
		Where(sq.Expr("cats_a.tags && ARRAY(?)",
			sq.Select("tag_values_a.id").From("tag_values_a").
				Join("tags_a ON tag_values_a.tag_id = tags_a.id").
				Where(sq.And{
					sq.Eq{"tags_a.name": "age"},
					sq.GtOrEq{"tag_values_a.value": 10},
				}),
		)).
		Limit(queryLimit).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	d2, err := runSQLQuery("array column (3 tags)", query)
	if err != nil {
		return nil, err
	}
	return []time.Duration{d1, d2}, nil
}
