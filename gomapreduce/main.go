package main

import (
	"fmt"
	"gomapreduce/mapreduce"
)

func main() {
	text := "hello world hello mapreduce world"
	words := mapreduce.Map(text)
	counts := mapreduce.Reduce(words)
	fmt.Println("Input:", text)
	fmt.Println("Word counts:")
	for w, c := range counts {
		fmt.Printf("%s: %d\n", w, c)
	}
}
