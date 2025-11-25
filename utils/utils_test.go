// SPDX-FileCopyrightText: 2023 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package utils

import (
	"os"
	"testing"

	"github.com/uyuni-project/inter-server-sync/tests"
)

func TestArrayRevert(t *testing.T) {
	myArray := []int{1, 2, 3}
	myArrayRevert := make([]int, len(myArray))
	copy(myArrayRevert, myArray)
	ReverseArray(myArrayRevert)
	for i, value := range myArray {
		if myArrayRevert[len(myArray)-i-1] != value {
			t.Fatalf("values are different: %d -> %d", myArrayRevert[len(myArray)-i-1], value) // to indicate test failed
		}
	}
}

func TestValidateDateValid(t *testing.T) {
	date := "2022-01-01"
	validatedDate, ok := ValidateDate(date)
	if !ok {
		t.Errorf("The date is not validated properly.")
	}
	if date != validatedDate {
		t.Errorf("The date is not validated properly.")
	}
}

func TestValidateDateInvalid(t *testing.T) {
	date := ""
	validatedDate, ok := ValidateDate(date)
	if !ok {
		t.Errorf("The date should be valid.")
	}
	if validatedDate != "" {
		t.Errorf("The date is not validated properly.")
	}
}

func TestGetCurrentServerFQDN43(t *testing.T) {
	rhnconf := `
# OSA configuration #

server.jabber_server = myhostname.example.com
osa-dispatcher.jabber_server = myhostname.example.com

# set up SSL on the dispatcher
osa-dispatcher.osa_ssl_cert = /srv/www/htdocs/pub/RHN-ORG-TRUSTED-SSL-CERT

# system snapshots enabled
enable_snapshots = 1

#cobbler host name
cobbler.host = myhostname.example.com

# Maximum Java Heap Size (in MB)
# taskomatic.java.maxmemory=4096

# Extended reposync filters to use the entire NEVRA
server.satellite.reposync_nevra_filter = 0
#option generated from rhn-config-satellite.pl
disconnected=1

#option generated from rhn-config-satellite.pl
product_name=SUSE Manager
	`
	tmpFile := tests.CreateTempFile(t, rhnconf)
	defer os.Remove(tmpFile)

	fqdn, err := GetCurrentServerFQDN(tmpFile)
	if err != nil {
		t.Error("Error during hostname lookup")
	}
	if fqdn != "myhostname.example.com" {
		t.Error("Wrong hostname found")
	}
}

func TestGetCurrentServerFQDN(t *testing.T) {
	rhnconf := `
# OSA configuration #

java.hostname = myhostname.example.com
osa-dispatcher.jabber_server = myhostname.example.com

# set up SSL on the dispatcher
osa-dispatcher.osa_ssl_cert = /srv/www/htdocs/pub/RHN-ORG-TRUSTED-SSL-CERT

# system snapshots enabled
enable_snapshots = 1

#cobbler host name
cobbler.host = localhost

# Maximum Java Heap Size (in MB)
# taskomatic.java.maxmemory=4096
	`
	tmpFile := tests.CreateTempFile(t, rhnconf)
	defer os.Remove(tmpFile)

	fqdn, err := GetCurrentServerFQDN(tmpFile)
	if err != nil {
		t.Error("Error during hostname lookup")
	}
	if fqdn != "myhostname.example.com" {
		t.Error("Wrong hostname found")
	}
}

func TestGetCurrentServerFQDNInvalid(t *testing.T) {
	rhnconf := `
	# OSA configuration #

server.jabber_server = myhostname.example.com
osa-dispatcher.jabber_server = myhostname.example.com

# set up SSL on the dispatcher
osa-dispatcher.osa_ssl_cert = /srv/www/htdocs/pub/RHN-ORG-TRUSTED-SSL-CERT

# system snapshots enabled
enable_snapshots = 1

# Maximum Java Heap Size (in MB)
# taskomatic.java.maxmemory=4096
	`
	tmpFile := tests.CreateTempFile(t, rhnconf)
	defer os.Remove(tmpFile)

	fqdn, err := GetCurrentServerFQDN(tmpFile)
	if err == nil {
		t.Error("Unexpected success for FQDN lookup")
	}
	if fqdn != "" {
		t.Error("Hostname found even when not supposed to")
	}
}
