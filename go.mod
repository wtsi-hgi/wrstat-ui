module github.com/wtsi-ssg/wrstat

go 1.16

require (
	github.com/VertebrateResequencing/wr v0.24.1-0.20210611100658-112c45bd87e0
	github.com/dgryski/go-farm v0.0.0-20200201041132-a6ae2369ad13
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/smartystreets/goconvey v1.6.4
	github.com/spf13/cobra v1.1.3
)

// we need to specify these due to github.com/VertebrateResequencing/wr's deps
replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0
