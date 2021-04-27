package schemareader

import (
	"bufio"
	"database/sql"
	"fmt"
	"os"
	"strings"

	"github.com/rs/zerolog/log"
)

type dataSource struct {
	host     string
	port     string
	dbname   string
	user     string
	password string
}

// GetConnectionString return the connection string for the database after reading config file for
func GetConnectionString(configFilePath string) string {
	file, err := os.Open(configFilePath)
	if err != nil {
		log.Fatal().Err(err).Msg("error loading configuration file")
		panic(err)
	}
	defer file.Close()

	dataSource := &dataSource{}
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if equal := strings.Index(line, "="); equal >= 0 {
			value := ""
			if len(line) > equal {
				value = strings.TrimSpace(line[equal+1:])
			}
			if key := strings.TrimSpace(line[:equal]); len(key) > 0 {
				switch key {
				case "db_host":
					dataSource.host = value
				case "db_port":
					dataSource.port = value
				case "db_name":
					dataSource.dbname = value
				case "db_user":
					dataSource.user = value
				case "db_password":
					dataSource.password = value

				}
			}
		}
	}
	return fmt.Sprintf("user='%s' password='%s' dbname='%s' host='%s' port='%s' sslmode=disable", dataSource.user, dataSource.password, dataSource.dbname, dataSource.host, dataSource.port)
}

//GetDBconnection return the database connection
func GetDBconnection(configFilePath string) *sql.DB {
	db, err := sql.Open("postgres", GetConnectionString(configFilePath))
	if err != nil {
		log.Fatal().Err(err).Msg("error getting connection to the database")
		panic(err)
	}
	return db
}
