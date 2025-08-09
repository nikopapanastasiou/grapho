package main

import (
	"fmt"
	"grapho/catalog"
)

func main() {
	// at server boot:
	store, _ := catalog.NewFileStore("./meta/catalog")
	reg, _ := catalog.Open(store)

	// use in executor for DDL:
	ev := catalog.DDLEvent{
		Op: catalog.OpCreateNode,
		Stmt: catalog.CreateNodePayload{
			Name: "Person",
			Fields: []catalog.FieldPayload{
				{Name: "id", Type: catalog.TypeSpec{Base: catalog.BaseUUID}, PrimaryKey: true},
				{Name: "email", Type: catalog.TypeSpec{Base: catalog.BaseString}, Unique: true},
			},
		},
	}
	newCat, err := reg.Apply(ev)
	_ = newCat
	_ = err

	// reads (lock-free):
	cur := reg.Current()
	if nt, ok := cur.Nodes["Person"]; ok {
		fmt.Println(nt)
	}

}
