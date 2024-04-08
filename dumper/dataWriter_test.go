// SPDX-FileCopyrightText: 2024 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package dumper

import (
	"testing"
	"time"

	"github.com/uyuni-project/inter-server-sync/sqlUtil"
)

func TestFormatField(t *testing.T) {
	tests := []struct {
		col         sqlUtil.RowDataStructure
		expectedVal string
	}{
		// Test case for NULL value
		{
			col:         sqlUtil.RowDataStructure{},
			expectedVal: "null",
		},
		// Test case for NUMERIC column type
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "NUMERIC", Value: "10"},
			expectedVal: "10",
		},
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "NUMERIC", Value: "-10"},
			expectedVal: "-10",
		},
		// Test case for TIMESTAMPTZ and TIMESTAMP column types
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "TIMESTAMPTZ", Value: time.Date(1984, time.July, 9, 17, 20, 0, 0, time.UTC)},
			expectedVal: "'1984-07-09 17:20:00Z'",
		},
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "TIMESTAMPTZ", Value: time.Date(2019, time.May, 29, 13, 49, 0, 0, time.FixedZone("UTC-1", -3600))},
			expectedVal: "'2019-05-29 13:49:00-01:00'",
		},
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "TIMESTAMPTZ", Value: time.Date(2021, time.June, 25, 0, 56, 0, 0, time.FixedZone("UTC+1", 3600))},
			expectedVal: "'2021-06-25 00:56:00+01:00'",
		},
		// Test case for SQL column type
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "SQL", Value: "SELECT * FROM table"},
			expectedVal: "(SELECT * FROM table)",
		},
		// Test case for BYTEA column type
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "BYTEA", Value: []byte("hello")},
			expectedVal: "decode('68656c6c6f', 'hex')",
		},
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "BYTEA", Value: []byte("\"\\[\\e[0;32m\\]\\u@\\h:\\w\\$ \\[\\e[m\\]\ntest\"")},
			expectedVal: "decode('225c5b5c655b303b33326d5c5d5c75405c683a5c775c24205c5b5c655b6d5c5d0a7465737422', 'hex')",
		},
		// Test case for default column type
		{
			col:         sqlUtil.RowDataStructure{ColumnType: "DEFAULT", Value: "default"},
			expectedVal: "'default'",
		},
	}

	for _, test := range tests {
		result := formatField(test.col)
		if result != test.expectedVal {
			t.Errorf("formatField(%+v) = %s; expected %s", test.col, result, test.expectedVal)
		}
	}
}
