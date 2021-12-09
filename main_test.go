package main

import (
	"os"
	"testing"
)

func TestAppStartsWithArguments(t *testing.T) {

	// Arrange
	// set command lines args
	name := os.Args[0]
	os.Args = []string{
		name,
		"help",
	}

	// Act
	main()

	// Assert
	// checks for exit code implicitly
}
