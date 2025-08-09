package catalog

import (
	"reflect"
	"testing"
)

func TestTypeSpecCloning(t *testing.T) {
	tests := []struct {
		name string
		ts   TypeSpec
	}{
		{
			name: "simple scalar",
			ts:   TypeSpec{Base: BaseString},
		},
		{
			name: "array type",
			ts:   TypeSpec{Base: BaseArray, Elem: &TypeSpec{Base: BaseInt}},
		},
		{
			name: "enum type",
			ts:   TypeSpec{Base: BaseEnum, EnumVals: []string{"a", "b", "c"}},
		},
		{
			name: "nested array",
			ts: TypeSpec{
				Base: BaseArray,
				Elem: &TypeSpec{
					Base: BaseArray,
					Elem: &TypeSpec{Base: BaseString},
				},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cloned := cloneType(tt.ts)
			
			// Should be equal
			if !reflect.DeepEqual(tt.ts, cloned) {
				t.Errorf("cloned type not equal: got %+v, want %+v", cloned, tt.ts)
			}
			
			// Modifications to clone shouldn't affect original
			if cloned.Elem != nil {
				cloned.Elem.Base = BaseBool // modify clone
				if tt.ts.Elem != nil && tt.ts.Elem.Base == BaseBool {
					t.Error("modifying clone affected original")
				}
			}
			
			if len(cloned.EnumVals) > 0 {
				cloned.EnumVals[0] = "modified"
				if len(tt.ts.EnumVals) > 0 && tt.ts.EnumVals[0] == "modified" {
					t.Error("modifying clone enum vals affected original")
				}
			}
		})
	}
}

func TestFieldSpecCloning(t *testing.T) {
	defaultVal := "test"
	fs := FieldSpec{
		Name:       "testField",
		Type:       TypeSpec{Base: BaseString, EnumVals: []string{"a", "b"}},
		Unique:     true,
		NotNull:    true,
		DefaultRaw: &defaultVal,
	}
	
	// Clone via NodeType cloning
	nt := &NodeType{
		Name:   "Test",
		Fields: map[string]FieldSpec{"test": fs},
		PK:     "test",
		Indexes: map[string]IndexSpec{"test": {Field: "test", Unique: true}},
	}
	
	cloned := cloneNodeType(nt)
	
	if !reflect.DeepEqual(nt.Fields["test"], cloned.Fields["test"]) {
		t.Error("field spec not properly cloned")
	}
	
	// Modify clone's default
	*cloned.Fields["test"].DefaultRaw = "modified"
	if *nt.Fields["test"].DefaultRaw == "modified" {
		t.Error("modifying clone's default affected original")
	}
	
	// Modify clone's enum vals
	clonedField := cloned.Fields["test"]
	clonedField.Type.EnumVals[0] = "modified"
	cloned.Fields["test"] = clonedField
	
	if nt.Fields["test"].Type.EnumVals[0] == "modified" {
		t.Error("modifying clone's enum vals affected original")
	}
}

func TestNodeTypeCloning(t *testing.T) {
	defaultVal := "default"
	nt := &NodeType{
		Name: "Person",
		Fields: map[string]FieldSpec{
			"id": {
				Name:    "id",
				Type:    TypeSpec{Base: BaseUUID},
				NotNull: true,
			},
			"name": {
				Name:       "name",
				Type:       TypeSpec{Base: BaseString},
				DefaultRaw: &defaultVal,
			},
		},
		PK: "id",
		Indexes: map[string]IndexSpec{
			"id": {Field: "id", Unique: true},
		},
	}
	
	cloned := cloneNodeType(nt)
	
	if !reflect.DeepEqual(nt, cloned) {
		t.Error("node type not properly cloned")
	}
	
	// Verify deep cloning - modify clone
	cloned.Name = "Modified"
	cloned.Fields["id"] = FieldSpec{Name: "modified"}
	cloned.PK = "modified"
	cloned.Indexes["new"] = IndexSpec{Field: "new"}
	
	if nt.Name == "Modified" {
		t.Error("modifying clone name affected original")
	}
	if nt.Fields["id"].Name == "modified" {
		t.Error("modifying clone field affected original")
	}
	if nt.PK == "modified" {
		t.Error("modifying clone PK affected original")
	}
	if _, exists := nt.Indexes["new"]; exists {
		t.Error("modifying clone indexes affected original")
	}
}

func TestEdgeTypeCloning(t *testing.T) {
	et := &EdgeType{
		Name: "WORKS_AT",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Company", Card: One},
		Props: map[string]FieldSpec{
			"role": {
				Name: "role",
				Type: TypeSpec{Base: BaseString},
			},
		},
	}
	
	cloned := cloneEdgeType(et)
	
	if !reflect.DeepEqual(et, cloned) {
		t.Error("edge type not properly cloned")
	}
	
	// Verify deep cloning
	cloned.Name = "Modified"
	cloned.From.Label = "Modified"
	cloned.Props["role"] = FieldSpec{Name: "modified"}
	
	if et.Name == "Modified" || et.From.Label == "Modified" {
		t.Error("modifying clone affected original")
	}
	if et.Props["role"].Name == "modified" {
		t.Error("modifying clone props affected original")
	}
}

func TestCatalogCloning(t *testing.T) {
	cat := &Catalog{
		Version: 5,
		Nodes: map[string]*NodeType{
			"Person": {
				Name:   "Person",
				Fields: map[string]FieldSpec{},
				PK:     "",
			},
		},
		Edges: map[string]*EdgeType{
			"KNOWS": {
				Name: "KNOWS",
				From: EdgeEndpoint{Label: "Person", Card: Many},
				To:   EdgeEndpoint{Label: "Person", Card: Many},
			},
		},
	}
	
	cloned := cat.Clone()
	
	if cloned.Version != cat.Version {
		t.Error("version not cloned correctly")
	}
	
	// Verify deep cloning
	cloned.Version = 10
	cloned.Nodes["Person"].Name = "Modified"
	cloned.Edges["KNOWS"].Name = "Modified"
	
	if cat.Version == 10 {
		t.Error("modifying clone version affected original")
	}
	if cat.Nodes["Person"].Name == "Modified" {
		t.Error("modifying clone node affected original")
	}
	if cat.Edges["KNOWS"].Name == "Modified" {
		t.Error("modifying clone edge affected original")
	}
}

func TestNewEmpty(t *testing.T) {
	cat := NewEmpty()
	
	if cat.Version != 0 {
		t.Errorf("expected version 0, got %d", cat.Version)
	}
	if cat.Nodes == nil || len(cat.Nodes) != 0 {
		t.Error("expected empty nodes map")
	}
	if cat.Edges == nil || len(cat.Edges) != 0 {
		t.Error("expected empty edges map")
	}
}

func TestCardinalityConstants(t *testing.T) {
	if One != 0 || Many != 1 {
		t.Error("cardinality constants have unexpected values")
	}
}

func TestBaseTypeConstants(t *testing.T) {
	// Just verify they're distinct
	types := []BaseType{
		BaseString, BaseText, BaseInt, BaseFloat, BaseBool,
		BaseUUID, BaseDate, BaseTime, BaseDateTime, BaseJSON,
		BaseBlob, BaseArray, BaseEnum,
	}
	
	seen := make(map[BaseType]bool)
	for _, bt := range types {
		if seen[bt] {
			t.Errorf("duplicate base type value: %d", bt)
		}
		seen[bt] = true
	}
}
