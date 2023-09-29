package postgrestest

import (
	"context"
	"database/sql"
	"log"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
)

const initJoinTables = `
DROP TABLE IF EXISTS cats_j;
DROP TABLE IF EXISTS tags_j;
DROP TABLE IF EXISTS cat_tags_j;
DROP INDEX IF EXISTS cats_j_id;
DROP INDEX IF EXISTS tags_j_id;
DROP INDEX IF EXISTS cat_tags_j_id;
CREATE TABLE cats_j(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE tags_j(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
CREATE TABLE cat_tags_j(cat_id INTEGER NOT NULL, tag_id INTEGER NOT NULL, value VARCHAR NOT NULL);
CREATE INDEX cats_j_id ON cats_j(id);
CREATE INDEX tags_j_id ON tags_j(id);
CREATE INDEX cat_tags_j_id ON cat_tags_j(tag_id);
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

func QueryJoinTable(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats_j").
		Join("cat_tags_j tagv1 ON cats_j.id = tagv1.cat_id").
		Join("tags_j tagn1 ON tagv1.tag_id = tagn1.id").
		Where(sq.Eq{
			"tagn1.name":  "color",
			"tagv1.value": "brown",
		}).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("join table (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats_j").
		Join("cat_tags_j tagv1 ON cats_j.id = tagv1.cat_id").
		Join("cat_tags_j tagv2 ON cats_j.id = tagv2.cat_id").
		Join("cat_tags_j tagv3 ON cats_j.id = tagv3.cat_id").
		Join("tags_j tagn1 ON tagv1.tag_id = tagn1.id").
		Join("tags_j tagn2 ON tagv2.tag_id = tagn2.id").
		Join("tags_j tagn3 ON tagv3.tag_id = tagn3.id").
		Where(sq.And{
			sq.Eq{
				"tagn1.name":  "color",
				"tagv1.value": "brown",
				"tagn2.name":  "age",
				"tagn3.name":  "demeanor",
				"tagv3.value": "grumpy",
			},
			sq.GtOrEq{
				"tagv2.value": "4",
			},
		}).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("join table (3 tags)", query); err != nil {
		return err
	}
	return nil
}
