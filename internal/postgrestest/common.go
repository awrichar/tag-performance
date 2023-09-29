package postgrestest

import (
	"database/sql"
	"log"
	"time"

	"dev/tagperformance/internal/common"

	sq "github.com/Masterminds/squirrel"
	_ "github.com/lib/pq"
)

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

func buildTagMap(tx *sql.Tx, tags []*common.Tag, table string) (map[string]int, error) {
	ins := sq.Insert(table).Columns("name").PlaceholderFormat(sq.Dollar)
	tagMap := make(map[string]int, len(tags))
	for _, tag := range tags {
		ins = ins.Values(tag.Name)
	}
	rows, err := ins.Suffix("RETURNING id, name").RunWith(tx).Query()
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var id int
		var name string
		if err := rows.Scan(&id, &name); err != nil {
			return nil, err
		}
		tagMap[name] = id
	}
	return tagMap, nil
}
