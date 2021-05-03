package schemareader

import (
	"fmt"
	"testing"
)

var configPath = "rhn.test"

func TestGetConnectionString(t *testing.T) {
	configTestString := "user='spacewalk' password='spacewalk' dbname='susemanager' host='192.168.122.177' port='5432' sslmode=disable"
	if GetConnectionString(configPath) != configTestString {
		t.Fatal("Reading from configfile failed")
	}
}

func TestGetDBconnection(t *testing.T) {
	fmt.Printf("%#v", GetDBconnection(configPath))
}
