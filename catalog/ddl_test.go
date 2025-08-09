package catalog

import (
	"strings"
	"testing"
)

func TestApplyCreateNodeSuccess(t *testing.T) {
	cat := NewEmpty()

	payload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{
				Name:       "id",
				Type:       TypeSpec{Base: BaseUUID},
				PrimaryKey: true,
				NotNull:    true,
			},
			{
				Name:    "email",
				Type:    TypeSpec{Base: BaseString},
				Unique:  true,
				NotNull: true,
			},
			{
				Name:       "name",
				Type:       TypeSpec{Base: BaseString},
				DefaultRaw: stringPtr("Anonymous"),
			},
			{
				Name: "status",
				Type: TypeSpec{
					Base:     BaseEnum,
					EnumVals: []string{"active", "inactive", "pending"},
				},
				DefaultRaw: stringPtr("pending"),
			},
		},
	}

	newCat, err := ApplyCreateNode(cat, payload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newCat.Version != 1 {
		t.Errorf("expected version 1, got %d", newCat.Version)
	}

	node, exists := newCat.Nodes["Person"]
	if !exists {
		t.Fatal("Person node not created")
	}

	if node.Name != "Person" {
		t.Errorf("expected name Person, got %s", node.Name)
	}

	if node.PK != "id" {
		t.Errorf("expected PK id, got %s", node.PK)
	}

	if len(node.Fields) != 4 {
		t.Errorf("expected 4 fields, got %d", len(node.Fields))
	}

	// Check id field
	idField := node.Fields["id"]
	if idField.Type.Base != BaseUUID || !idField.NotNull {
		t.Error("id field not configured correctly")
	}

	// Check email field
	emailField := node.Fields["email"]
	if !emailField.Unique || !emailField.NotNull {
		t.Error("email field not configured correctly")
	}

	// Check name field with default
	nameField := node.Fields["name"]
	if nameField.DefaultRaw == nil || *nameField.DefaultRaw != "Anonymous" {
		t.Error("name field default not set correctly")
	}

	// Check enum field
	statusField := node.Fields["status"]
	if len(statusField.Type.EnumVals) != 3 || statusField.Type.EnumVals[0] != "active" {
		t.Error("status enum field not configured correctly")
	}

	// Check indexes
	if len(node.Indexes) != 2 {
		t.Errorf("expected 2 indexes, got %d", len(node.Indexes))
	}

	idIndex, exists := node.Indexes["id"]
	if !exists || !idIndex.Unique || idIndex.Field != "id" {
		t.Error("id index not created correctly")
	}

	emailIndex, exists := node.Indexes["email"]
	if !exists || !emailIndex.Unique || emailIndex.Field != "email" {
		t.Error("email index not created correctly")
	}
}

func TestApplyCreateNodeValidationErrors(t *testing.T) {
	cat := NewEmpty()

	tests := []struct {
		name    string
		payload CreateNodePayload
		wantErr string
	}{
		{
			name:    "empty name",
			payload: CreateNodePayload{Name: ""},
			wantErr: "node name required",
		},
		{
			name:    "no fields",
			payload: CreateNodePayload{Name: "Test", Fields: []FieldPayload{}},
			wantErr: "node must define at least one field",
		},
		{
			name: "duplicate field names",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{Name: "id", Type: TypeSpec{Base: BaseString}},
					{Name: "id", Type: TypeSpec{Base: BaseInt}},
				},
			},
			wantErr: "duplicate field",
		},
		{
			name: "empty field name",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{Name: "", Type: TypeSpec{Base: BaseString}},
				},
			},
			wantErr: "field with empty name",
		},
		{
			name: "multiple primary keys",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{Name: "id1", Type: TypeSpec{Base: BaseString}, PrimaryKey: true},
					{Name: "id2", Type: TypeSpec{Base: BaseString}, PrimaryKey: true},
				},
			},
			wantErr: "multiple PRIMARY KEY fields",
		},
		{
			name: "non-scalar primary key",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{
						Name:       "id",
						Type:       TypeSpec{Base: BaseArray, Elem: &TypeSpec{Base: BaseString}},
						PrimaryKey: true,
					},
				},
			},
			wantErr: "primary key \"id\" must be scalar",
		},
		{
			name: "not null with null default",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{
						Name:       "field",
						Type:       TypeSpec{Base: BaseString},
						NotNull:    true,
						DefaultRaw: stringPtr("null"),
					},
				},
			},
			wantErr: "field \"field\" NOT NULL but default null",
		},
		{
			name: "enum with no values",
			payload: CreateNodePayload{
				Name: "Test",
				Fields: []FieldPayload{
					{
						Name: "status",
						Type: TypeSpec{Base: BaseEnum, EnumVals: []string{}},
					},
				},
			},
			wantErr: "enum field \"status\" must have values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ApplyCreateNode(cat, tt.payload)
			if err == nil {
				t.Fatal("expected error but got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestApplyCreateNodeDuplicateName(t *testing.T) {
	cat := NewEmpty()

	payload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseString}},
		},
	}

	// First creation should succeed
	cat, err := ApplyCreateNode(cat, payload)
	if err != nil {
		t.Fatalf("first creation failed: %v", err)
	}

	// Second creation should fail
	_, err = ApplyCreateNode(cat, payload)
	if err == nil {
		t.Fatal("expected error for duplicate node name")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestApplyCreateEdgeSuccess(t *testing.T) {
	cat := NewEmpty()

	// Create prerequisite nodes
	personPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	companyPayload := CreateNodePayload{
		Name: "Company",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}

	cat, _ = ApplyCreateNode(cat, personPayload)
	cat, _ = ApplyCreateNode(cat, companyPayload)

	edgePayload := CreateEdgePayload{
		Name: "WORKS_AT",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Company", Card: One},
		Props: []FieldPayload{
			{
				Name: "role",
				Type: TypeSpec{Base: BaseString},
			},
			{
				Name: "start_date",
				Type: TypeSpec{Base: BaseDate},
			},
			{
				Name: "level",
				Type: TypeSpec{
					Base:     BaseEnum,
					EnumVals: []string{"junior", "senior", "lead"},
				},
				DefaultRaw: stringPtr("junior"),
			},
		},
	}

	newCat, err := ApplyCreateEdge(cat, edgePayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newCat.Version != 3 { // 2 nodes + 1 edge
		t.Errorf("expected version 3, got %d", newCat.Version)
	}

	edge, exists := newCat.Edges["WORKS_AT"]
	if !exists {
		t.Fatal("WORKS_AT edge not created")
	}

	if edge.From.Label != "Person" || edge.From.Card != Many {
		t.Error("FROM endpoint not configured correctly")
	}

	if edge.To.Label != "Company" || edge.To.Card != One {
		t.Error("TO endpoint not configured correctly")
	}

	if len(edge.Props) != 3 {
		t.Errorf("expected 3 props, got %d", len(edge.Props))
	}

	roleField := edge.Props["role"]
	if roleField.Type.Base != BaseString {
		t.Error("role prop not configured correctly")
	}

	levelField := edge.Props["level"]
	if len(levelField.Type.EnumVals) != 3 || levelField.DefaultRaw == nil || *levelField.DefaultRaw != "junior" {
		t.Error("level enum prop not configured correctly")
	}
}

func TestApplyCreateEdgeValidationErrors(t *testing.T) {
	cat := NewEmpty()

	// Create a node for valid references
	nodePayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseString}},
		},
	}
	cat, _ = ApplyCreateNode(cat, nodePayload)

	tests := []struct {
		name    string
		payload CreateEdgePayload
		wantErr string
	}{
		{
			name:    "empty name",
			payload: CreateEdgePayload{Name: ""},
			wantErr: "edge name required",
		},
		{
			name: "missing FROM node",
			payload: CreateEdgePayload{
				Name: "TEST",
				From: EdgeEndpoint{Label: "NonExistent"},
				To:   EdgeEndpoint{Label: "Person"},
			},
			wantErr: "FROM node type \"NonExistent\" not found",
		},
		{
			name: "missing TO node",
			payload: CreateEdgePayload{
				Name: "TEST",
				From: EdgeEndpoint{Label: "Person"},
				To:   EdgeEndpoint{Label: "NonExistent"},
			},
			wantErr: "TO node type \"NonExistent\" not found",
		},
		{
			name: "duplicate prop names",
			payload: CreateEdgePayload{
				Name: "TEST",
				From: EdgeEndpoint{Label: "Person"},
				To:   EdgeEndpoint{Label: "Person"},
				Props: []FieldPayload{
					{Name: "prop", Type: TypeSpec{Base: BaseString}},
					{Name: "prop", Type: TypeSpec{Base: BaseInt}},
				},
			},
			wantErr: "duplicate edge prop",
		},
		{
			name: "empty prop name",
			payload: CreateEdgePayload{
				Name: "TEST",
				From: EdgeEndpoint{Label: "Person"},
				To:   EdgeEndpoint{Label: "Person"},
				Props: []FieldPayload{
					{Name: "", Type: TypeSpec{Base: BaseString}},
				},
			},
			wantErr: "edge prop with empty name",
		},
		{
			name: "enum prop with no values",
			payload: CreateEdgePayload{
				Name: "TEST",
				From: EdgeEndpoint{Label: "Person"},
				To:   EdgeEndpoint{Label: "Person"},
				Props: []FieldPayload{
					{
						Name: "status",
						Type: TypeSpec{Base: BaseEnum, EnumVals: []string{}},
					},
				},
			},
			wantErr: "enum prop \"status\" must have values",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ApplyCreateEdge(cat, tt.payload)
			if err == nil {
				t.Fatal("expected error but got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestApplyCreateEdgeDuplicateName(t *testing.T) {
	cat := NewEmpty()

	// Create prerequisite node
	nodePayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseString}},
		},
	}
	cat, _ = ApplyCreateNode(cat, nodePayload)

	edgePayload := CreateEdgePayload{
		Name: "KNOWS",
		From: EdgeEndpoint{Label: "Person"},
		To:   EdgeEndpoint{Label: "Person"},
	}

	// First creation should succeed
	cat, err := ApplyCreateEdge(cat, edgePayload)
	if err != nil {
		t.Fatalf("first creation failed: %v", err)
	}

	// Second creation should fail
	_, err = ApplyCreateEdge(cat, edgePayload)
	if err == nil {
		t.Fatal("expected error for duplicate edge name")
	}
	if !strings.Contains(err.Error(), "already exists") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestIsScalarType(t *testing.T) {
	scalarTypes := []BaseType{
		BaseString, BaseText, BaseInt, BaseFloat, BaseBool,
		BaseUUID, BaseDate, BaseTime, BaseDateTime,
	}

	nonScalarTypes := []BaseType{
		BaseJSON, BaseBlob, BaseArray, BaseEnum,
	}

	for _, bt := range scalarTypes {
		if !isScalarType(TypeSpec{Base: bt}) {
			t.Errorf("expected %v to be scalar", bt)
		}
	}

	for _, bt := range nonScalarTypes {
		if isScalarType(TypeSpec{Base: bt}) {
			t.Errorf("expected %v to not be scalar", bt)
		}
	}
}

func TestApplyAlterNodeAddField(t *testing.T) {
	cat := NewEmpty()

	// Create initial node
	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	// Add a field
	alterPayload := AlterNodePayload{
		Name: "Person",
		Actions: []NodeAlterAction{
			{
				Type: "ADD_FIELD",
				Field: &FieldPayload{
					Name:    "email",
					Type:    TypeSpec{Base: BaseString},
					Unique:  true,
					NotNull: true,
				},
			},
		},
	}

	newCat, err := ApplyAlterNode(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newCat.Version != 2 {
		t.Errorf("expected version 2, got %d", newCat.Version)
	}

	node := newCat.Nodes["Person"]
	if len(node.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(node.Fields))
	}

	emailField := node.Fields["email"]
	if !emailField.Unique || !emailField.NotNull {
		t.Error("email field not configured correctly")
	}

	if _, exists := node.Indexes["email"]; !exists {
		t.Error("unique index not created for email field")
	}
}

func TestApplyAlterNodeDropField(t *testing.T) {
	cat := NewEmpty()

	// Create initial node with multiple fields
	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
			{Name: "email", Type: TypeSpec{Base: BaseString}, Unique: true},
			{Name: "name", Type: TypeSpec{Base: BaseString}},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	// Drop a field
	alterPayload := AlterNodePayload{
		Name: "Person",
		Actions: []NodeAlterAction{
			{
				Type:      "DROP_FIELD",
				FieldName: "name",
			},
		},
	}

	newCat, err := ApplyAlterNode(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := newCat.Nodes["Person"]
	if len(node.Fields) != 2 {
		t.Errorf("expected 2 fields, got %d", len(node.Fields))
	}

	if _, exists := node.Fields["name"]; exists {
		t.Error("name field should have been dropped")
	}
}

func TestApplyAlterNodeDropPrimaryKeyField(t *testing.T) {
	cat := NewEmpty()

	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	alterPayload := AlterNodePayload{
		Name: "Person",
		Actions: []NodeAlterAction{
			{
				Type:      "DROP_FIELD",
				FieldName: "id",
			},
		},
	}

	_, err := ApplyAlterNode(cat, alterPayload)
	if err == nil {
		t.Fatal("expected error when dropping primary key field")
	}
	if !strings.Contains(err.Error(), "cannot drop primary key field") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestApplyAlterNodeModifyField(t *testing.T) {
	cat := NewEmpty()

	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
			{Name: "name", Type: TypeSpec{Base: BaseString}},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	alterPayload := AlterNodePayload{
		Name: "Person",
		Actions: []NodeAlterAction{
			{
				Type: "MODIFY_FIELD",
				Field: &FieldPayload{
					Name:    "name",
					Type:    TypeSpec{Base: BaseText},
					NotNull: true,
					Unique:  true,
				},
			},
		},
	}

	newCat, err := ApplyAlterNode(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := newCat.Nodes["Person"]
	nameField := node.Fields["name"]
	if nameField.Type.Base != BaseText || !nameField.NotNull || !nameField.Unique {
		t.Error("name field not modified correctly")
	}

	if _, exists := node.Indexes["name"]; !exists {
		t.Error("unique index not created for modified field")
	}
}

func TestApplyAlterNodeSetPrimaryKey(t *testing.T) {
	cat := NewEmpty()

	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "email", Type: TypeSpec{Base: BaseString}, Unique: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	alterPayload := AlterNodePayload{
		Name: "Person",
		Actions: []NodeAlterAction{
			{
				Type:      "SET_PRIMARY_KEY",
				FieldName: "email",
			},
		},
	}

	newCat, err := ApplyAlterNode(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := newCat.Nodes["Person"]
	if node.PK != "email" {
		t.Errorf("expected PK email, got %s", node.PK)
	}
}

func TestApplyAlterNodeValidationErrors(t *testing.T) {
	cat := NewEmpty()

	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	tests := []struct {
		name    string
		payload AlterNodePayload
		wantErr string
	}{
		{
			name:    "empty node name",
			payload: AlterNodePayload{Name: ""},
			wantErr: "node name required",
		},
		{
			name:    "nonexistent node",
			payload: AlterNodePayload{Name: "NonExistent", Actions: []NodeAlterAction{{Type: "ADD_FIELD"}}},
			wantErr: "does not exist",
		},
		{
			name:    "no actions",
			payload: AlterNodePayload{Name: "Person", Actions: []NodeAlterAction{}},
			wantErr: "at least one action required",
		},
		{
			name: "add existing field",
			payload: AlterNodePayload{
				Name: "Person",
				Actions: []NodeAlterAction{
					{
						Type:  "ADD_FIELD",
						Field: &FieldPayload{Name: "id", Type: TypeSpec{Base: BaseString}},
					},
				},
			},
			wantErr: "already exists",
		},
		{
			name: "drop nonexistent field",
			payload: AlterNodePayload{
				Name: "Person",
				Actions: []NodeAlterAction{
					{Type: "DROP_FIELD", FieldName: "nonexistent"},
				},
			},
			wantErr: "does not exist",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ApplyAlterNode(cat, tt.payload)
			if err == nil {
				t.Fatal("expected error but got none")
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Errorf("expected error containing %q, got %q", tt.wantErr, err.Error())
			}
		})
	}
}

func TestApplyAlterEdgeAddProp(t *testing.T) {
	cat := NewEmpty()

	// Create prerequisite nodes
	nodePayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, nodePayload)

	// Create edge
	edgePayload := CreateEdgePayload{
		Name: "KNOWS",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Person", Card: Many},
	}
	cat, _ = ApplyCreateEdge(cat, edgePayload)

	// Add property to edge
	alterPayload := AlterEdgePayload{
		Name: "KNOWS",
		Actions: []EdgeAlterAction{
			{
				Type: "ADD_PROP",
				Prop: &FieldPayload{
					Name: "since",
					Type: TypeSpec{Base: BaseDate},
				},
			},
		},
	}

	newCat, err := ApplyAlterEdge(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	edge := newCat.Edges["KNOWS"]
	if len(edge.Props) != 1 {
		t.Errorf("expected 1 prop, got %d", len(edge.Props))
	}

	sinceField := edge.Props["since"]
	if sinceField.Type.Base != BaseDate {
		t.Error("since prop not configured correctly")
	}
}

func TestApplyAlterEdgeChangeEndpoint(t *testing.T) {
	cat := NewEmpty()

	// Create nodes
	personPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	companyPayload := CreateNodePayload{
		Name: "Company",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, personPayload)
	cat, _ = ApplyCreateNode(cat, companyPayload)

	// Create edge
	edgePayload := CreateEdgePayload{
		Name: "WORKS_AT",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Person", Card: One}, // Initially Person -> Person
	}
	cat, _ = ApplyCreateEdge(cat, edgePayload)

	// Change TO endpoint
	alterPayload := AlterEdgePayload{
		Name: "WORKS_AT",
		Actions: []EdgeAlterAction{
			{
				Type:        "CHANGE_ENDPOINT",
				Endpoint:    "TO",
				NewEndpoint: &EdgeEndpoint{Label: "Company", Card: One},
			},
		},
	}

	newCat, err := ApplyAlterEdge(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	edge := newCat.Edges["WORKS_AT"]
	if edge.To.Label != "Company" {
		t.Errorf("expected TO endpoint Company, got %s", edge.To.Label)
	}
}

func TestApplyDropNode(t *testing.T) {
	cat := NewEmpty()

	// Create node
	createPayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	// Drop node
	dropPayload := DropNodePayload{Name: "Person"}

	newCat, err := ApplyDropNode(cat, dropPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newCat.Version != 2 {
		t.Errorf("expected version 2, got %d", newCat.Version)
	}

	if _, exists := newCat.Nodes["Person"]; exists {
		t.Error("Person node should have been dropped")
	}
}

func TestApplyDropNodeReferencedByEdge(t *testing.T) {
	cat := NewEmpty()

	// Create nodes and edge
	nodePayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, nodePayload)

	edgePayload := CreateEdgePayload{
		Name: "KNOWS",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Person", Card: Many},
	}
	cat, _ = ApplyCreateEdge(cat, edgePayload)

	// Try to drop node referenced by edge
	dropPayload := DropNodePayload{Name: "Person"}

	_, err := ApplyDropNode(cat, dropPayload)
	if err == nil {
		t.Fatal("expected error when dropping node referenced by edge")
	}
	if !strings.Contains(err.Error(), "referenced by edge") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestApplyDropEdge(t *testing.T) {
	cat := NewEmpty()

	// Create prerequisite node and edge
	nodePayload := CreateNodePayload{
		Name: "Person",
		Fields: []FieldPayload{
			{Name: "id", Type: TypeSpec{Base: BaseUUID}, PrimaryKey: true},
		},
	}
	cat, _ = ApplyCreateNode(cat, nodePayload)

	edgePayload := CreateEdgePayload{
		Name: "KNOWS",
		From: EdgeEndpoint{Label: "Person", Card: Many},
		To:   EdgeEndpoint{Label: "Person", Card: Many},
	}
	cat, _ = ApplyCreateEdge(cat, edgePayload)

	// Drop edge
	dropPayload := DropEdgePayload{Name: "KNOWS"}

	newCat, err := ApplyDropEdge(cat, dropPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if newCat.Version != 3 {
		t.Errorf("expected version 3, got %d", newCat.Version)
	}

	if _, exists := newCat.Edges["KNOWS"]; exists {
		t.Error("KNOWS edge should have been dropped")
	}
}

func TestApplyDropNonexistentNode(t *testing.T) {
	cat := NewEmpty()

	dropPayload := DropNodePayload{Name: "NonExistent"}

	_, err := ApplyDropNode(cat, dropPayload)
	if err == nil {
		t.Fatal("expected error when dropping nonexistent node")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestApplyDropNonexistentEdge(t *testing.T) {
	cat := NewEmpty()

	dropPayload := DropEdgePayload{Name: "NonExistent"}

	_, err := ApplyDropEdge(cat, dropPayload)
	if err == nil {
		t.Fatal("expected error when dropping nonexistent edge")
	}
	if !strings.Contains(err.Error(), "does not exist") {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestComplexAlterNodeScenario(t *testing.T) {
	cat := NewEmpty()

	// Create initial node
	createPayload := CreateNodePayload{
		Name: "User",
		Fields: []FieldPayload{
			{Name: "email", Type: TypeSpec{Base: BaseString}, Unique: true},
			{Name: "name", Type: TypeSpec{Base: BaseString}},
		},
	}
	cat, _ = ApplyCreateNode(cat, createPayload)

	// Complex alter: add field, modify field, set primary key
	alterPayload := AlterNodePayload{
		Name: "User",
		Actions: []NodeAlterAction{
			{
				Type: "ADD_FIELD",
				Field: &FieldPayload{
					Name:    "id",
					Type:    TypeSpec{Base: BaseUUID},
					NotNull: true,
				},
			},
			{
				Type: "MODIFY_FIELD",
				Field: &FieldPayload{
					Name:    "name",
					Type:    TypeSpec{Base: BaseText},
					NotNull: true,
				},
			},
			{
				Type:      "SET_PRIMARY_KEY",
				FieldName: "id",
			},
		},
	}

	newCat, err := ApplyAlterNode(cat, alterPayload)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	node := newCat.Nodes["User"]

	// Check id field was added and set as PK
	if node.PK != "id" {
		t.Errorf("expected PK id, got %s", node.PK)
	}

	idField := node.Fields["id"]
	if !idField.NotNull {
		t.Error("id field should be NOT NULL")
	}

	// Check name field was modified
	nameField := node.Fields["name"]
	if nameField.Type.Base != BaseText || !nameField.NotNull {
		t.Error("name field not modified correctly")
	}

	// Check indexes
	if len(node.Indexes) != 2 { // id (PK) and email (unique)
		t.Errorf("expected 2 indexes, got %d", len(node.Indexes))
	}
}

// Helper function
func stringPtr(s string) *string {
	return &s
}
