/*******************************************************************************
 * Copyright (c) 2022, 2025 Genome Research Ltd.
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

package db

// Error is the custom error type for the db package.
type Error string

const (
	// ErrDBExists is returned when attempting to create a database that already exists.
	ErrDBExists = Error("database already exists")
	// ErrDBNotExists is returned when attempting to open a database that doesn't exist.
	ErrDBNotExists = Error("database doesn't exist")
	// ErrDirNotFound is returned when a directory is not found in the database.
	ErrDirNotFound = Error("directory not found")
)

func (e Error) Error() string { return string(e) }

// DBInfo holds summary information about a database.
type DBInfo struct { //nolint:revive
	NumDirs     int
	NumDGUTAs   int
	NumParents  int
	NumChildren int
}
