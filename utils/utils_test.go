package utils

import (
	"testing"
)

func TestArrayRevert(t *testing.T) {
	myArray := []int{1,2,3}
	myArrayRevert := make([]int, len(myArray))
	copy(myArrayRevert, myArray)
	ReverseArray(myArrayRevert)
	for i, value := range myArray {
		if myArrayRevert[len(myArray) - i - 1] != value {
			t.Fatalf("values are different: %d -> %d",  myArrayRevert[len(myArray) - i - 1],value) // to indicate test failed
		}
	}
}
