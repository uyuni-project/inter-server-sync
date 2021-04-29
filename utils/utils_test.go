package utils

import (
	"testing"
)

func TestReverseArray(t *testing.T) {
	testArray := []int{1,2,3,4,5}
	reverttestArray := make([]int, len(testArray))
	arrayLength := copy(reverttestArray, testArray)
	ReverseArray(reverttestArray)
	for i, v := range testArray {
		if reverttestArray[arrayLength - i - 1 ] !=v {
			t.Fatal("ReverseArray function failed")
		}
	}
}

func TestContains(t *testing.T) {
	testSlice := []string{"Test", "Toast", "Telephone"}
	element := "Toast"
	if Contains(testSlice, element) != true {
		t.Fatal("Contains method failed")
	}
}