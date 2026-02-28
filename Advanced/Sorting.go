package advanced

import (
	"fmt"
	"slices"
)

func SortingInGo() {
	strs := []string{"a", "d", "b", "c"}
	slices.Sort(strs)
	fmt.Println("Strings:", strs)

	ints := []int{7, 2, 4}
	slices.Sort(ints)
	fmt.Println("Ints:", ints)

	s := slices.IsSorted(ints)
	fmt.Println("Sorted", s)

}
