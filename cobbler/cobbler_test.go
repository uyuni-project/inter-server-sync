// SPDX-FileCopyrightText: 2025 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package cobbler

import (
	"testing"
)

func TestMakeCobblerName(t *testing.T) {
	tests := []struct {
		inname   string
		org      string
		orgid    string
		expected string
	}{
		{"name", "org", "1", "name:S:1:org"},
		{"name with spaces", "org with spaces", "1", "name_with_spaces:S:1:orgwithspaces"},
		{"name!@#", "org!@#", "1", "name:S:1:org"},
		{"name.dot", "org-dash", "1", "name.dot:S:1:org-dash"},
	}

	for _, tt := range tests {
		result := makeCobblerName(tt.inname, tt.org, tt.orgid)
		if result != tt.expected {
			t.Errorf("makeCobblerName(%q, %q, %q) = %q, want %q", tt.inname, tt.org, tt.orgid, result, tt.expected)
		}
	}
}

func TestMakeCobblerNameVR(t *testing.T) {
	tests := []struct {
		name       string
		version    string
		revision   string
		org        string
		orgid      string
		expectedVR string
		expectedV  string
	}{
		{"name", "1.0", "1", "org", "1", "name-1.0-1:S:1:org", "name-1.0:S:1:org"},
		{"name with spaces", "1.0", "1", "org with spaces", "1", "name_with_spaces-1.0-1:S:1:orgwithspaces", "name_with_spaces-1.0:S:1:orgwithspaces"},
	}

	for _, tt := range tests {
		resVR, resV := makeCobblerNameVR(tt.name, tt.version, tt.revision, tt.org, tt.orgid)
		if resVR != tt.expectedVR {
			t.Errorf("makeCobblerNameVR(%q, %q, %q, %q, %q) VR = %q, want %q", tt.name, tt.version, tt.revision, tt.org, tt.orgid, resVR, tt.expectedVR)
		}
		if resV != tt.expectedV {
			t.Errorf("makeCobblerNameVR(%q, %q, %q, %q, %q) V = %q, want %q", tt.name, tt.version, tt.revision, tt.org, tt.orgid, resV, tt.expectedV)
		}
	}
}
