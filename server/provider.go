package server

import (
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Provider bundles the backend implementations required by the server.
//
// Reloading is an implementation detail of each Provider.
type Provider interface {
	// Tree returns a query object used by tree + where endpoints.
	Tree() *db.Tree

	// BaseDirs returns a query object used by basedirs endpoints.
	BaseDirs() basedirs.Reader

	// OnUpdate registers a callback that is called whenever underlying data
	// changes such that server caches should be rebuilt.
	OnUpdate(cb func())

	Close() error
}
