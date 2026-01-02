/*******************************************************************************
 * Copyright (c) 2025 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
 *   Michael Woolnough <mw31@sanger.ac.uk>
 *
 * Permission is hereby granted, free of charge, to any person obtaining
 * a copy of this software and associated documentation files (the
 * "Software"), to deal in the Software without restriction, including
 * without limitation the rights to use, copy, modify, merge, publish,
 * distribute, sublicense, and/or sell copies of the Software, and to
 * permit persons to whom the Software is furnished to do so, subject to
 * the following conditions:
 *
 * The above copyright notice and this permission notice shall be included
 * in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
 * IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY
 * CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT,
 * TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
 * SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 ******************************************************************************/

// Package provider defines the Provider interface used by the server package.
// This package exists to break the import cycle between bolt and server.
package provider

import (
	"github.com/wtsi-hgi/wrstat-ui/basedirs"
	"github.com/wtsi-hgi/wrstat-ui/db"
)

// Error is our custom error type.
type Error string

// ErrNoPaths is returned when no database paths are found.
const ErrNoPaths = Error("no db paths found")

func (e Error) Error() string { return string(e) }

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
