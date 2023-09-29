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

func insertBatchArrayColumn(tx *sql.Tx, batch []*common.Cat, tagValueMap map[string]string) error {
	inserts := make([]string, len(batch))
	args := make([]interface{}, 0, len(batch))
	for i, cat := range batch {
		catTags := make([]string, len(cat.Tags))
		for j, tag := range cat.Tags {
			catTags[j] = tagValueMap[fmt.Sprintf("%s:%s", tag.Name, tag.Value)]
		}
		args = append(args, cat.Name)
		args = append(args, "{"+strings.Join(catTags, ",")+"}")
		inserts[i] = fmt.Sprintf("($%d, $%d)", len(args)-1, len(args))
	}
	_, err := tx.Exec("INSERT INTO cats_array(name, tags) VALUES "+strings.Join(inserts, ","), args...)
	return err
}

func SetupArrayColumn(db *sql.DB, cats []*common.Cat, tags []*common.Tag) error {
	log.Print("Building array column")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(`
        DROP TABLE IF EXISTS cats_array;
		DROP TABLE IF EXISTS tags;
		DROP TABLE IF EXISTS tag_values;
        DROP INDEX IF EXISTS cats_array_x;
		DROP INDEX IF EXISTS tags_x;
		DROP INDEX IF EXISTS tag_values_x;
		DROP INDEX IF EXISTS tag_values_name;
        CREATE TABLE cats_array(name VARCHAR NOT NULL, tags INTEGER[]);
		CREATE TABLE tags(id SERIAL PRIMARY KEY, name VARCHAR NOT NULL);
		CREATE TABLE tag_values(id SERIAL PRIMARY KEY, tag_id INTEGER NOT NULL, value VARCHAR NOT NULL);
        CREATE INDEX cats_array_x ON cats_array USING GIN(tags);
		CREATE INDEX tags_x ON tags(id);
		CREATE INDEX tag_values_x ON tag_values(id);
		CREATE UNIQUE INDEX tag_values_name ON tag_values(tag_id, value);
		`)
	if err != nil {
		return err
	}
	tagMap, err := buildTagMap(tx, tags)
	if err != nil {
		return err
	}
	tagValueMap := make(map[string]string, 0)
	for _, tag := range tags {
		for _, val := range tag.Values {
			result, err := tx.Query("INSERT INTO tag_values(tag_id, value) VALUES($1, $2) RETURNING id", tagMap[tag.Name], val)
			if err != nil {
				return err
			}
			result.Next()
			var valID string
			if err := result.Scan(&valID); err != nil {
				return err
			}
			result.Close()
			tagValueMap[fmt.Sprintf("%s:%s", tag.Name, val)] = valID
		}
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

func QueryArrayColumn(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats_array").
		Where(
			sq.Expr("cats_array.tags @> ARRAY(?)",
				sq.Select("tag_values.id").From("tag_values").
					Join("tags ON tag_values.tag_id = tags.id").
					Where(sq.Eq{
						"tags.name":        "color",
						"tag_values.value": "brown",
					}).PlaceholderFormat(sq.Dollar),
			),
		).
		RunWith(db)
	if err := runSQLQuery("array column (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats_array").
		Where(
			sq.And{
				sq.Expr("cats_array.tags && ARRAY(?)",
					sq.Select("tag_values.id").From("tag_values").
						Join("tags ON tag_values.tag_id = tags.id").
						Where(sq.Eq{
							"tags.name":        "color",
							"tag_values.value": "brown",
						}),
				),
				sq.Expr("cats_array.tags && ARRAY(?)",
					sq.Select("tag_values.id").From("tag_values").
						Join("tags ON tag_values.tag_id = tags.id").
						Where(sq.Eq{
							"tags.name":        "demeanor",
							"tag_values.value": "grumpy",
						}),
				),
				sq.Expr("cats_array.tags && ARRAY(?)",
					sq.Select("tag_values.id").From("tag_values").
						Join("tags ON tag_values.tag_id = tags.id").
						Where(sq.And{
							sq.Eq{"tags.name": "age"},
							sq.GtOrEq{"tag_values.value": "4"},
						}),
				),
			},
		).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("array column (3 tags)", query); err != nil {
		return err
	}
	return nil
}
