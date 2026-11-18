package index

import (
	"lamina/internal/catalog"
	"lamina/internal/versions"
)

type Registry struct{ Trees map[string]*Tree }

func Build(c *catalog.Catalog, s *versions.Store) *Registry {
	r := &Registry{Trees: map[string]*Tree{}}
	for name, definition := range c.Indexes {
		tree := New()
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted {
					tree.Add(v.Row[definition.Column], key)
				}
			}
		}
		r.Trees[name] = tree
	}
	return r
}
func (r *Registry) Apply(c *catalog.Catalog, writes []versions.Mutation) {
	for _, m := range writes {
		if m.Deleted {
			continue
		}
		for name, i := range c.Indexes {
			if i.Relation == m.Relation {
				r.Trees[name].Add(m.Row[i.Column], m.Key)
			}
		}
	}
}

// RebuildRelations reconstructs index trees only for the relations whose
// version chains changed. Reclamation is budgeted by relation, so rebuilding
// only those relations keeps the maintenance pass proportional to the work it
// was allowed to do instead of rescanning the whole store.
func (r *Registry) RebuildRelations(c *catalog.Catalog, s *versions.Store, relations []string) {
	if len(relations) == 0 {
		return
	}
	touched := make(map[string]bool, len(relations))
	for _, name := range relations {
		touched[name] = true
	}
	for name, definition := range c.Indexes {
		if !touched[definition.Relation] {
			continue
		}
		tree := New()
		for key, chain := range s.Tables[definition.Relation] {
			for _, v := range chain {
				if !v.Deleted {
					tree.Add(v.Row[definition.Column], key)
				}
			}
		}
		r.Trees[name] = tree
	}
}
