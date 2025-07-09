// SPDX-FileCopyrightText: 2025 SUSE LLC
//
// SPDX-License-Identifier: Apache-2.0

package cmd

import (
	"os"
	"testing"
)

// create a temp file with a dummy password
func createTempFile(t *testing.T) string {
	t.Helper()
	f, err := os.CreateTemp("", "passwordfile")
	if err != nil {
		t.Fatalf("failed to create temp file: %v", err)
	}
	_, err = f.WriteString("filepassword\n")
	if err != nil {
		t.Fatalf("failed to write to temp file: %v", err)
	}
	f.Close()
	return f.Name()
}

// TestGetXMLRPCPassword tests the getXMLRPCPassword function
// Checks the priority of password sources: xmlRpcPasswordFile flag, stdin, and xmlRpcPassword flag.
func TestGetXMLRPCPassword(t *testing.T) {
	// Create the temp file once
	tmpFile := createTempFile(t)
	defer os.Remove(tmpFile)

	tests := []struct {
		xmlRpcPassword     string
		xmlRpcPasswordFile string
		stdinContent       string
		expected           string
	}{
		// dont provide any password, get the same as xmlRpcPassword
		{
			xmlRpcPassword:     "",
			xmlRpcPasswordFile: "",
			stdinContent:       "",
			expected:           "",
		},
		{
			xmlRpcPassword:     "abc",
			xmlRpcPasswordFile: "",
			stdinContent:       "",
			expected:           "abc",
		},
		//stdin takes prio over xmlRpcPassword, handles \n
		{
			xmlRpcPassword:     "flagpassword",
			xmlRpcPasswordFile: "",
			stdinContent:       "stdinpassword\n",
			expected:           "stdinpassword",
		},
		// xmlRpcPasswordFile takes prio over stdin (which took prio over xmlRpcPassword)
		{
			xmlRpcPassword:     "flagpassword",
			xmlRpcPasswordFile: tmpFile,
			stdinContent:       "stdinpassword\n",
			expected:           "filepassword",
		},
	}

	for i, tt := range tests {
		origStdin := os.Stdin
		defer func() { os.Stdin = origStdin }()

		// Prepare stdin
		if tt.stdinContent != "" {
			r, w, err := os.Pipe()
			if err != nil {
				t.Fatalf("failed to create pipe: %v", err)
			}
			go func() {
				w.Write([]byte(tt.stdinContent))
				w.Close()
			}()
			os.Stdin = r
		} else {
			f, err := os.Open("/dev/null")
			if err != nil {
				t.Fatalf("failed to open /dev/null: %v", err)
			}
			os.Stdin = f
		}

		got, err := getXMLRPCPassword(tt.xmlRpcPassword, tt.xmlRpcPasswordFile)
		if err != nil {
			t.Fatalf("test case %d: unexpected error: %v", i, err)
		}
		if got != tt.expected {
			t.Errorf("test case %d: expected '%s', got '%s'", i, tt.expected, got)
		}
	}
}

func TestGetXMLRPCPassword_FileNotFound(t *testing.T) {
	_, err := getXMLRPCPassword("", "./nonexistentfile")
	if err == nil {
		t.Fatal("expected error, got nil")
	}
}
