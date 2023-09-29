package common

import "fmt"

func PrintCounter(counter int) {
	if counter%10000 == 0 {
		fmt.Printf("%d ", counter)
		if counter%100000 == 0 {
			fmt.Println("")
		}
	}
}
