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

## Database connection configuration

Database connection configuration are loaded by default from `/etc/rhn/rhn.conf`.
File location can be overwritten. 
For development environments one can use a sample file in this project.

Steps:
1. copy sample file `cp rhn.conf.exaple rhn.conf`
2. fill all properties in `rhn.conf` with the appropriated values
3. use this configuration file by specifying the config parameter: `go run . -config=rhn.conf`


## Export Data

- run commnad `go run -channels=LABEL1,LABEL2`

A file named `sql_statements.sql` will be generated on the location defined in `path`.

For debug purposes it's also possible to generate debug information about the generated data.
`go run -channels=LABEL1,LABEL2 -debug`

## After export

Copy file to target machine and run `sql_statements.sql`
Import can be done with `spacewalk-sql sql_statements.sql`

## Profile
Run with profile: `go run . -cpuprofile=cpu.prof -memprofile=mem.prof ...`

View Profile data: `go tool pprof -web mem.prof`