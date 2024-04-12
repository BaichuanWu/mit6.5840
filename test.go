package main

import (
	"fmt"
	"strings"
)

type Work struct {
	name string
}

type Task struct {
	works []Work
}

func (t *Task) getWork() Work {
	for idx, w:=range t.works {
		w.name = "aaaaa"
		t.works[idx].name = "ccccccc"
		return w
	}
	return Work{}
}

func main() {
	a := "ssss8781"
	b := strings.TrimSuffix(a, "81")
	fmt.Println(a,b)
	works := make([]Work, 1)
	works[0] = Work{"sss"}
	t:=Task{works: works}
	w := t.getWork()
	fmt.Println(w)
	w.name = "eeee"
	fmt.Println(t)
	c := make([][]int, 10)
	fmt.Println(c)
}
