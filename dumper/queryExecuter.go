package dumper

import (
	"database/sql"
	"log"
	"reflect"
)

func executeQueryWithResults(db *sql.DB, sql string, scanParameters ...interface{}) [][]rowDataStructure {

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
		// allocate reflect.Value representing a **T value
		rowValues[i] = reflect.New(reflect.PtrTo(columnTypes[i].ScanType()))
	}

	computedValues := make([][]rowDataStructure, 0)
	for rows.Next() {
		// initially will hold pointers for Scan, after scanning the
		// pointers will be dereferenced so that the slice holds actual values
		rowResult := make([]interface{}, len(columnTypes))
		for i := 0; i < len(columnTypes); i++ {
			// get the **T value from the reflect.Value
			rowResult[i] = rowValues[i].Interface()
		}

		// scan each column value into the corresponding **T value
		if err := rows.Scan(rowResult...); err != nil {
			log.Fatal(err)
		}

		// dereference pointers
		rowComputedValues := make([]rowDataStructure, 0)
		for i := 0; i < len(rowValues); i++ {
			// first pointer deref to get reflect.Value representing a *T value,
			// if rv.IsNil it means column value was NULL
			if rv := rowValues[i].Elem(); rv.IsNil() {
				rowResult[i] = nil
			} else {
				// second deref to get reflect.Value representing the T value
				// and call Interface to get T value from the reflect.Value
				rowResult[i] = rv.Elem().Interface()
			}
			rowComputedValues = append(rowComputedValues, rowDataStructure{columnType: columnTypes[i].DatabaseTypeName(),
				initialValue: rowResult[i], value: rowResult[i], columnName: columnTypes[i].Name()})
		}

		computedValues = append(computedValues, rowComputedValues)
	}

	return computedValues
}
