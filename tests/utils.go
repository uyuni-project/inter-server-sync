package tests

import (
	"bufio"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
)

// DataRepository encapsulates I/O operations.
type DataRepository struct {
	DB         *sql.DB
	mock       sqlmock.Sqlmock
	Writer     *bufio.Writer
	mockWriter *MockWriter
}

// CreateDataRepository factory method for the DataRepository
func CreateDataRepository() *DataRepository {
	db, mock, err := sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	mock.MatchExpectationsInOrder(true)
	checkErr(err)

	mockWriter := &MockWriter{}
	writerAdapter := bufio.NewWriter(mockWriter)
	return &DataRepository{DB: db, mock: mock, Writer: writerAdapter, mockWriter: mockWriter}
}

// Expect adds data to repository, which can then be retrieved by the tested function.
func (repo *DataRepository) Expect(stm string, numRecords int, args ...driver.Value) {

	// simulate table having only one row
	recs := sqlmock.
		NewRows([]string{"id", "fk_id"})

	for i := 0; i < numRecords; i++ {
		recs = recs.AddRow(fmt.Sprintf("%04d", i+1), fmt.Sprintf("%04d", 1))
	}
	// add mock expectation
	if len(args) > 0 {
		repo.mock.
			ExpectQuery(stm).
			WithArgs(args...).
			WillReturnRows(recs).
			RowsWillBeClosed()
	} else {
		repo.mock.
			ExpectQuery(stm).
			WillReturnRows(recs).
			RowsWillBeClosed()
	}

}

// ExpectationsWereMet checks whether all queued expectations
// were met in order. If any of them was not met - an error is returned.
func (repo *DataRepository) ExpectationsWereMet() error {
	return repo.mock.ExpectationsWereMet()
}

func (repo *DataRepository) GetWriterBuffer() []string {
	err := repo.Writer.Flush()
	checkErr(err)
	return repo.mockWriter.data
}

// MockWriter allows to create a mock bufferWriter object, as it implements the interface
type MockWriter struct {
	data []string
}

func (mr *MockWriter) Write(p []byte) (n int, err error) {

	mr.data = append(mr.data, string(p))
	return 4096, nil
}

func (mr *MockWriter) GetData() []string {
	return mr.data
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}
