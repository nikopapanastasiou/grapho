package main

import (
	"fmt"
	"grapho/parser"
)

func main() {
	stmts, errs := parser.NewParser("CREATE NODE A(id:int); CREATE NODE B(id:int);").ParseScript()
	fmt.Println(stmts)
	fmt.Println(errs)
}
