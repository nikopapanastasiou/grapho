package catalog

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
)

// Store abstracts snapshot/log persistence.
type Store interface {
	Load() (*Catalog, uint64 /*ddloffset*/, error)
	AppendDDL(ev DDLEvent) (newOffset uint64, err error) // SYNC
	Snapshot(cat *Catalog) error                         // SYNC
	UpdateManifest(catVersion uint64, ddlOffset uint64) error
}

type Registry struct {
	store Store

	cur atomic.Pointer[Catalog] // immutable snapshot for readers

	muW       sync.Mutex // serialize writers (DDL)
	ddlOffset uint64
}

// Open initializes the registry by loading snapshot and replaying DDL log.
func Open(store Store) (*Registry, error) {
	cat, off, err := store.Load()
	if err != nil {
		return nil, err
	}
	if cat == nil {
		cat = NewEmpty()
	}
	r := &Registry{store: store, ddlOffset: off}
	r.cur.Store(cat)
	return r, nil
}

func (r *Registry) Current() *Catalog {
	return r.cur.Load()
}

// Apply validates, persists DDL (SYNC), and publishes a new catalog snapshot atomically.
func (r *Registry) Apply(ev DDLEvent) (*Catalog, error) {
	r.muW.Lock()
	defer r.muW.Unlock()

	// 1) Compute the new catalog in memory (copy-on-write)
	old := r.cur.Load()
	var (
		newCat *Catalog
		err    error
	)
	switch ev.Op {
	case OpCreateNode:
		var p CreateNodePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyCreateNode(old, p)
	case OpCreateEdge:
		var p CreateEdgePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyCreateEdge(old, p)
	case OpAlterNode:
		var p AlterNodePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyAlterNode(old, p)
	case OpAlterEdge:
		var p AlterEdgePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyAlterEdge(old, p)
	case OpDropNode:
		var p DropNodePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyDropNode(old, p)
	case OpDropEdge:
		var p DropEdgePayload
		if err := decode(ev.Stmt, &p); err != nil {
			return nil, err
		}
		newCat, err = ApplyDropEdge(old, p)
	default:
		return nil, fmt.Errorf("unsupported DDL op %s", ev.Op)
	}
	if err != nil {
		return nil, err
	}

	// 2) Persist the DDL event synchronously
	off, err := r.store.AppendDDL(ev)
	if err != nil {
		return nil, err
	}

	// 3) Publish the new catalog snapshot for readers
	r.cur.Store(newCat)
	r.ddlOffset = off

	// 4) Update manifest (best effort but recommended to be SYNC as well)
	if err := r.store.UpdateManifest(newCat.Version, off); err != nil {
		return nil, err
	}
	return newCat, nil
}

func (r *Registry) Snapshot() error {
	return r.store.Snapshot(r.cur.Load())
}

func decode(src any, dst any) error {
	// src might already be the right type, or a map from JSON
	switch v := src.(type) {
	case nil:
		return fmt.Errorf("nil DDL payload")
	case map[string]any, []any:
		b, _ := json.Marshal(v)
		return json.Unmarshal(b, dst)
	default:
		// If the caller gave us a typed payload, use it
		b, _ := json.Marshal(v)
		return json.Unmarshal(b, dst)
	}
}
