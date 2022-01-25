package utils

import (
	"testing"
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
