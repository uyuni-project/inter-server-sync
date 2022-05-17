# Inter Server Sync (ISS)

[![Test](https://github.com/uyuni-project/inter-server-sync/actions/workflows/github-actions-tests.yml/badge.svg)](https://github.com/uyuni-project/inter-server-sync/actions/workflows/github-actions-tests.yml)

## Usage
run the command for more information:
`inter-server-sync -h`

## Known limitations 
- Source and target servers need to be on the same version.
- Export and import organization should have the same name.
- Export folder needs to be sync by hand to the target server.

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

## Build and release

### 1. Update cmd version

- Edit file `cmd/root.go` "Version" property to the desire version
- On project root folder run `osc vc` to update the changes file with the release data
- Manually update changes file with the release number for the next release
- commit and push to github

### 2. Create tag

- Create a tag with the version number using the format "v0.0.0" and push it to github
```
git tag v0.0.0
git push origin v0.0.0
```

### 3. Create a github release (optional)

- On github create a new version release based on the previous tag

### 4. OBS: project preparetion

- Projects names:
    - Uyuni: `systemsmanagement:Uyuni:Master`
    - Head: Devel: `Galaxy:Manager:Head`
    - Manager 4.2: `Devel:Galaxy:Manager:4.2`
- Pakcage name: `inter-server-sync`

On porject working directory: 

1. Adapt the `_services` file to be able to download the correct tag for the version
2. Run all services: `osc service runall`
3. Check the changes files is correctly updated
4. Check spec file was correctly updated with the release version
5. Add all files: `osc ar`
6. Remove old version files `tar` and `osinfo` (`osc rm filename`)
7. Commit everything with `osc commit`

### 5. OBS: create submit requests

Uyuni: `osc sr --no-cleanup <your_project> inter-server-sync systemsmanagement:Uyuni:Master`
Manager Head: `iosc sr --no-cleanup openSUSE.org:<your_project> inter-server-sync Devel:Galaxy:Manager:Head`
For each maintained SUSE Manager version, one SR in the form: `iosc sr --no-cleanup openSUSE.org:<your_project> inter-server-sync Devel:Galaxy:Manager:X.Y`
