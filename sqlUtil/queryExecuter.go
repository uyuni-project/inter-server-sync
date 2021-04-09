package sqlUtil

import (
	"database/sql"
	"log"
	"reflect"
)

type RowDataStructure struct {
	ColumnName   string
	ColumnType   string
	initialValue interface{}
	Value        interface{}
}

func (row RowDataStructure) GetInitialValue() interface{} {
	return row.initialValue
}

func ExecuteQueryWithResults(db *sql.DB, sql string, scanParameters ...interface{}) [][]RowDataStructure {

	rows, err := db.Query(sql, scanParameters...)

	if err != nil {
		log.Printf("Error : While executing '%s', with parameters %s", sql, scanParameters)
		log.Fatal(err)
	}
	defer rows.Close()

	// get column type info
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		log.Fatal(err)
	}

	// used for allocation & dereferencing
	rowValues := make([]reflect.Value, len(columnTypes))
	for i := 0; i < len(columnTypes); i++ {
		// allocate reflect.Value representing a **T Value
		rowValues[i] = reflect.New(reflect.PtrTo(columnTypes[i].ScanType()))
	}

	computedValues := make([][]RowDataStructure, 0)
	for rows.Next() {
		// initially will hold pointers for Scan, after scanning the
		// pointers will be dereferenced so that the slice holds actual values
		rowResult := make([]interface{}, len(columnTypes))
		for i := 0; i < len(columnTypes); i++ {
			// get the **T Value from the reflect.Value
			rowResult[i] = rowValues[i].Interface()
		}

		// scan each column Value into the corresponding **T Value
		if err := rows.Scan(rowResult...); err != nil {
			log.Fatal(err)
		}

		// dereference pointers
		rowComputedValues := make([]RowDataStructure, 0)
		for i := 0; i < len(rowValues); i++ {
			// first pointer deref to get reflect.Value representing a *T Value,
			// if rv.IsNil it means column Value was NULL
			if rv := rowValues[i].Elem(); rv.IsNil() {
				rowResult[i] = nil
			} else {
				// second deref to get reflect.Value representing the T Value
				// and call Interface to get T Value from the reflect.Value
				rowResult[i] = rv.Elem().Interface()
			}
			rowComputedValues = append(rowComputedValues, RowDataStructure{ColumnType: columnTypes[i].DatabaseTypeName(),
				initialValue: rowResult[i], Value: rowResult[i], ColumnName: columnTypes[i].Name()})
		}

		computedValues = append(computedValues, rowComputedValues)
	}

	return computedValues
}
