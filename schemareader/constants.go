// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package schemareader

const (
	ReadTableNames = `SELECT table_name
		FROM information_schema.tables
		WHERE table_schema = 'public'
			AND table_type = 'BASE TABLE';`

	ReadColumnNames = `SELECT column_name
		FROM information_schema.columns
		WHERE table_schema = 'public' AND table_name = $1
		ORDER BY ordinal_position;`

	ReadPkColumnNames = `SELECT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE  i.indrelid = $1::regclass
		AND    i.indisprimary;`

	ReadUniqueIndexNames = `SELECT DISTINCT indexrelid::regclass
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE i.indrelid = $1::regclass
		AND i.indisunique AND NOT i.indisprimary;`

	ReadIndexColumns = `SELECT DISTINCT a.attname
		FROM pg_index i
		JOIN pg_attribute a ON a.attrelid = i.indrelid
			AND a.attnum = ANY(i.indkey)
		WHERE indexrelid::regclass = $1::regclass;`

	ReadReferenceConstraintNames = `SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = $1;`

	ReadReferencedByConstraintNames = `SELECT DISTINCT tc.constraint_name
		FROM information_schema.table_constraints AS tc
			JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
				AND ccu.table_schema = tc.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY' AND ccu.table_name = $1;`

	ReadReferencedTable = `SELECT DISTINCT ccu.table_name
	FROM information_schema.constraint_column_usage AS ccu
	WHERE ccu.constraint_name = $1;`

	ReadReferencedByTable = `SELECT DISTINCT table_name
	FROM information_schema.table_constraints as tc 
	WHERE tc.constraint_name = $1;`

	ReadReferenceConstraints = `SELECT DISTINCT kcu.column_name, ccu.column_name AS foreign_column_name
		FROM information_schema.table_constraints AS tc
		JOIN information_schema.key_column_usage AS kcu ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		JOIN information_schema.constraint_column_usage AS ccu ON ccu.constraint_name = tc.constraint_name
			AND tc.table_schema = ccu.table_schema
		WHERE tc.constraint_type = 'FOREIGN KEY'
			AND tc.table_name = $1
			AND tc.constraint_name = $2;`

	ReadPkSequence = `WITH sequences AS (
		SELECT sequence_name
			FROM information_schema.sequences
			WHERE sequence_schema = 'public'
		),
		id_constraints AS (
			SELECT
				tc.constraint_name,
				tc.table_name,
				kcu.column_name
			FROM
				information_schema.table_constraints AS tc
				JOIN information_schema.key_column_usage AS kcu
					ON tc.constraint_name = kcu.constraint_name
			WHERE tc.constraint_schema = 'public'
				AND constraint_type = 'PRIMARY KEY'
				AND kcu.ordinal_position = 1
				AND column_name = 'id'
				AND tc.table_name = $1
		)
		SELECT sequence_name
			FROM id_constraints
			JOIN sequences
				ON replace(regexp_replace(constraint_name, '(_id)?_pk(ey)?', ''), '_', '') = replace(regexp_replace(sequence_name, '(_id)?_seq', ''), '_', '')`
)
