package mtimes

import (
	"io"
	"os"
	"path/filepath"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"github.com/wtsi-hgi/wrstat-ui/internal/statsdata"
	"vimagination.zapto.org/tree"
)

func TestFilter(t *testing.T) {
	Convey("With a mtime tree", t, func() {
		data := statsdata.NewRoot("/some/path/", 12345)

		data.AddDirectory("userDir").SetMeta(1, 1, 98765)
		statsdata.AddFile(data, "userDir/file1.txt", 1, 1, 0, 0, 98766)
		statsdata.AddFile(data, "userDir/file2.txt", 1, 2, 0, 0, 98767)
		statsdata.AddFile(data, "subDir/subsubDir/file3.txt", 3, 2, 0, 0, 98000)
		statsdata.AddFile(data, "subDir/othersubDir/file4.tsv", 3, 2, 0, 0, 97000)

		tmp := t.TempDir()
		file := filepath.Join(tmp, "data")
		out := filepath.Join(tmp, "tree.db")

		f, err := os.Create(file)
		So(err, ShouldBeNil)

		_, err = io.Copy(f, data.AsReader())
		So(err, ShouldBeNil)

		So(Build([]string{file}, out), ShouldBeNil)

		Convey("You can filter it", func() {
			tree, err := tree.OpenFile(out)
			So(err, ShouldBeNil)

			mtree, err := FilterTree(tree, GID(1))
			So(err, ShouldBeNil)

			So(mtree, ShouldResemble, &Tree{
				Name: "/",
				ChildTrees: []*Tree{
					{
						Name: "some/",
						ChildTrees: []*Tree{
							{
								Name: "path/",
								ChildTrees: []*Tree{
									{
										Name: "userDir/",
										ChildTrees: []*Tree{
											{
												Name:        "file1.txt",
												LatestMTime: 98766,
											},
										},
										LatestMTime: 98766,
									},
								},
								LatestMTime: 98766,
							},
						},
						LatestMTime: 98766,
					},
				},
				LatestMTime: 98766,
			})

			mtree, err = FilterTree(tree, UID(3))
			So(err, ShouldBeNil)

			So(mtree, ShouldResemble, &Tree{
				Name: "/",
				ChildTrees: []*Tree{
					{
						Name: "some/",
						ChildTrees: []*Tree{
							{
								Name: "path/",
								ChildTrees: []*Tree{
									{
										Name: "subDir/",
										ChildTrees: []*Tree{
											{
												Name: "othersubDir/",
												ChildTrees: []*Tree{
													{
														Name:        "file4.tsv",
														LatestMTime: 97000,
													},
												},
												LatestMTime: 97000,
											},
											{
												Name: "subsubDir/",
												ChildTrees: []*Tree{
													{
														Name:        "file3.txt",
														LatestMTime: 98000,
													},
												},
												LatestMTime: 98000,
											},
										},
										LatestMTime: 98000,
									},
								},
								LatestMTime: 98000,
							},
						},
						LatestMTime: 98000,
					},
				},
				LatestMTime: 98000,
			})
		})

		Convey("You can get a list of data directories and file locations", func() {
			tree, err := tree.OpenFile(out)
			So(err, ShouldBeNil)

			mtree, err := FilterTree(tree, GID(2))
			So(err, ShouldBeNil)

			So(mtree.ListFiles(), ShouldResemble, []PathTime{
				{Path: "/some/path/subDir/othersubDir/file4.tsv", MTime: 97000},
				{Path: "/some/path/subDir/subsubDir/file3.txt", MTime: 98000},
				{Path: "/some/path/userDir/file2.txt", MTime: 98767},
			})

			So(mtree.DataLocations(2), ShouldResemble, []PathTime{
				{Path: "/some/path/", MTime: 98767},
			})

			So(mtree.DataLocations(3), ShouldResemble, []PathTime{
				{Path: "/some/path/subDir/", MTime: 98000},
				{Path: "/some/path/userDir/", MTime: 98767},
			})
		})
	})
}
