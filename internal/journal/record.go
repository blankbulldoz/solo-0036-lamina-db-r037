package journal

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Record struct {
	Format      int                 `json:"format"`
	Sequence    uint64              `json:"sequence"`
	Kind        string              `json:"kind"`
	Transaction string              `json:"transaction,omitempty"`
	Relation    *catalog.Relation   `json:"relation,omitempty"`
	Index       *catalog.Index      `json:"index,omitempty"`
	Writes      []versions.Mutation `json:"writes,omitempty"`
	Horizon     uint64              `json:"horizon,omitempty"`
	// Vacuum work bounds. Zero means unbounded, which is also the meaning of
	// records written before bounded maintenance existed. The bounds are
	// journaled so replay releases exactly the same versions as the live pass.
	MaxKeys      int `json:"max_keys,omitempty"`
	MaxRelations int `json:"max_relations,omitempty"`
}
