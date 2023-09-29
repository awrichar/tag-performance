package postgrestest

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
)

const initJSONTables = `
DROP TABLE IF EXISTS cats_b;
DROP INDEX IF EXISTS cats_b_tags;
CREATE TABLE cats_b(name VARCHAR NOT NULL, tags JSONB);
CREATE INDEX cats_b_tags ON cats_b USING GIN(tags);
`

func insertBatchJSONColumn(tx *sql.Tx, batch []*common.Cat) error {
	ins := sq.Insert("cats_b").Columns("name", "tags").PlaceholderFormat(sq.Dollar)
	for _, cat := range batch {
		catTags := make(map[string]interface{}, len(cat.Tags))
		for _, tag := range cat.Tags {
			catTags[tag.Name] = tag.Value
		}
		catTagsJSON, err := json.Marshal(catTags)
		if err != nil {
			return err
		}
		ins = ins.Values(cat.Name, catTagsJSON)
	}
	_, err := ins.RunWith(tx).Exec()
	return err
}

func SetupJSONColumn(db *sql.DB, cats []*common.Cat, tags []*common.Tag) error {
	log.Print("Building jsonb column")
	tx, err := db.BeginTx(context.Background(), nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.Exec(initJSONTables)
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
			if err := insertBatchJSONColumn(tx, batch); err != nil {
				return err
			}
			batch = make([]*common.Cat, 0, batchMax)
		}
	}
	if len(batch) > 0 {
		if err := insertBatchJSONColumn(tx, batch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func QueryJSONColumn(db *sql.DB) error {
	query := sq.Select("COUNT(*)").From("cats_b").
		Where(sq.Expr("cats_b.tags @@ ?", `$.color == "brown"`)).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("jsonb column (1 tag)", query); err != nil {
		return err
	}
	query = sq.Select("COUNT(*)").From("cats_b").
		Where(sq.Expr(
			"cats_b.tags @@ ?",
			`$.color == "brown" && $.demeanor == "grumpy" && $.age >= 4`,
		)).
		PlaceholderFormat(sq.Dollar).
		RunWith(db)
	if err := runSQLQuery("jsonb column (3 tags)", query); err != nil {
		return err
	}
	return nil
}
