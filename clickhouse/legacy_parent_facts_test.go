//go:build legacy_parent_facts
// +build legacy_parent_facts

/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Authors:
 *   Sendu Bala <sb10@sanger.ac.uk>
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

package clickhouse

type NavigationObject string

const (
	NavigationObjectParentFacts NavigationObject = "wrstat_dirs"
	NavigationObjectChildFacts  NavigationObject = "wrstat_tree_nav_facts"
	NavigationObjectProjection  NavigationObject = "clickhouse_projection"
)

const (
	defaultChildrenBatchSize      = defaultProjectionBatchSize
	insertChildrenQuery           = "INSERT INTO wrstat_dirs"
	dropParentFactsPartitionQuery = "ALTER TABLE wrstat_dir_facts DROP PARTITION tuple(?, toUUID(?))"
	insertParentFactsQuery        = insertMountDirSummaryQuery
)

type parentFactChildSummary = childFilterAllSummary

func DefaultNavigationObject() NavigationObject {
	return NavigationObjectParentFacts
}

func ChooseNavigationObject(projectionAccepted, childFactsAccepted bool) NavigationObject {
	if projectionAccepted {
		return NavigationObjectProjection
	}

	if childFactsAccepted {
		return NavigationObjectChildFacts
	}

	return DefaultNavigationObject()
}

func parentFactsParentDir(dir string) string {
	return parentDirForPath(dir)
}

func parentFactsHasChildrenValue(childCount uint64) uint8 {
	if childCount > 0 {
		return 1
	}

	return 0
}

func parentFactsFallbackRouteName() string {
	return "parent_facts_fallback"
}

func parentFactsFallbackRoutes() uint64 {
	return 0
}

func resetParentFactsFallbackRoutesForTest() {}
