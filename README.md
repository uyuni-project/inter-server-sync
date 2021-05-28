# Inter Server Sync (ISS)

## Installation
Use the repository: https://download.opensuse.org/repositories/home:/RDiasMateus:/iss/

## Usage
run the command for more information:
`inter-server-sync -h`


### on source server
- **Create export dir**: `mkdir ~/export`
- **Run command**: `inter-server-sync export --serverConfig=/etc/rhn/rhn.conf --outputDir=~/export --channels=channel_label,channel_label`
- **Copy export directory to target server**: `rsync -r ~/export root@<Target_server>:~/`

### on target server
- **Run command: `inter-server-sync import --importDir ~/export/`

## Database connection configuration

Database connection configuration are loaded by default from `/etc/rhn/rhn.conf`.
File location can be overwritten.
For development environments one can use a sample file in this project.

Steps to run in locally in development mode:
1. copy sample file `cp rhn.conf.exaple rhn.conf`
2. fill all properties in `rhn.conf` with the appropriated values
3. use this configuration file by specifying the config parameter: `go run . -config=rhn.conf`

## Extra

### Dot graph with schema metadata

`go run . dot --serverConfig=rhn.conf |  dot -Tx11`

### Profile
Run with profile: `go run . -cpuprofile=cpu.prof -memprofile=mem.prof ...`

View Profile data: `go tool pprof -web mem.prof`

## Packaging

OBS project: https://build.opensuse.org/project/show/home:RDiasMateus:iss

## Service to create vendor sources
`osc service rundisabled`