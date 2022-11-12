package db_partitioner_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	partitioner "github.com/pashapdev/db_partitioner"

	"github.com/jackc/pgx/v4"
	dbCreater "github.com/pashapdev/db_creater"
	"github.com/stretchr/testify/require"
)

type testEntity struct {
	Content string
	Logdate time.Time
}

func connString(user, password, address, db string, port int) string {
	return fmt.Sprintf(
		"host=%s port=%d dbname=%s user=%s password=%s sslmode=disable",
		address,
		port,
		db,
		user,
		password)
}

func insertContent(ctx context.Context, conn *pgx.Conn, testEntities []testEntity) error {
	q := "INSERT INTO test_table(content, logdate) VALUES($1, $2)"

	for i := range testEntities {
		if _, err := conn.Exec(ctx, q, testEntities[i].Content, testEntities[i].Logdate); err != nil {
			return err
		}
	}
	return nil
}

func selectContent(ctx context.Context, conn *pgx.Conn) ([]testEntity, error) {
	rows, err := conn.Query(ctx, "SELECT content, logdate FROM test_table")
	if err != nil {
		return nil, err
	}

	var (
		testEntities []testEntity
		content      string
		logdate      time.Time
	)

	for rows.Next() {
		err := rows.Scan(&content, &logdate)
		if err != nil {
			return nil, err
		}
		testEntities = append(testEntities, testEntity{Content: content, Logdate: logdate})
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	defer rows.Close()

	return testEntities, nil
}

func now() time.Time {
	return time.Date(
		time.Now().Year(), time.Now().Month(), 1,
		0, 0, 0, 0, time.UTC)
}

func TestPartitioner(t *testing.T) {
	const (
		user      = "postgres"
		password  = "postgres"
		address   = "localhost"
		port      = 5432
		db        = "db_test"
		partTable = "test_table"
	)
	ctx := context.Background()

	creater := dbCreater.New(user, password, address, db, port)
	testDB, err := creater.CreateWithMigration("file://./migrations/")
	require.NoError(t, err)
	defer creater.Drop(testDB) //nolint:errcheck
	conn, err := pgx.Connect(ctx, connString(user, password, address, testDB, port))
	require.NoError(t, err)

	testData := []testEntity{
		{
			Content: "Content1",
			Logdate: now().AddDate(0, 1, 0),
		},
		{
			Content: "Content2",
			Logdate: now().AddDate(0, 2, 0),
		},
		{
			Content: "Content3",
			Logdate: now().AddDate(0, 3, 0),
		},
	}
	require.Error(t, insertContent(ctx, conn, testData))

	partitions := make([]partitioner.Partition, 12)
	startTime := time.Date(
		time.Now().Year(), time.Now().Month(), 1,
		0, 0, 0, 0, time.Local)
	startTime.Date()
	for i := 0; i < 12; i++ {
		partitions[i] = *partitioner.New(
			conn,
			partTable,
			partitioner.PartitionRange{
				From: startTime.Format("2006-01-02"),
				To:   startTime.AddDate(0, 1, 0).Format("2006-01-02"),
			})
		require.NoError(t, partitions[i].Create(ctx))
		startTime = startTime.AddDate(0, 1, 0)
	}

	require.NoError(t, insertContent(ctx, conn, testData))
	entities, err := selectContent(ctx, conn)
	require.NoError(t, err)
	require.ElementsMatch(t, entities, testData)
}
