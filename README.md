# Inter Server Sync (ISS)

## Available configuration parameters

| name       | default value       | description | 
| ---------- | ------------------- | ----------- |
| channels   | ""                  | Labels for channels to sync (comma seprated in case of multiple) |
| path       | "."                 | Location for generated data|
| config     | "/etc/rhn/rhn.conf" | Path for the config file | 
| dot        | false               | Output dot format of table metadata for Graphviz |
| debug      | false               | Output debug information about the export data |

## Dot graph with schema metadata

`go run . -dot | dot -Tx11`

## Export Data

- run commnad `go run -channels=LABEL1,LABEL2`

A file named `sql_statements.sql` will be generated on the location defined in `path`.

For debug purposes it's also possible to generate debug information about the generated data.
`go run -channels=LABEL1,LABEL2 -debug`

## After export

Copy file to target machine and run `sql-statements.sql`
Import can be done with `spacewalk-sql sql-statements.sql`