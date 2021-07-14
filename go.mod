module github.com/wtsi-ssg/wrstat

go 1.16

require (
	github.com/VertebrateResequencing/wr v0.24.1-0.20210611100658-112c45bd87e0
	github.com/hashicorp/go-multierror v1.1.1
	github.com/inconshreveable/log15 v0.0.0-20201112154412-8562bdadbbac
	github.com/karrick/godirwalk v1.16.1
	github.com/klauspost/pgzip v1.2.5
	github.com/phayes/freeport v0.0.0-20180830031419-95f893ade6f2
	github.com/rs/xid v1.3.0
	github.com/smartystreets/goconvey v1.6.4
	github.com/spf13/cobra v1.1.3
	github.com/termie/go-shutil v0.0.0-20140729215957-bcacb06fecae
	gopkg.in/yaml.v2 v2.4.0
)

// we need to specify these due to github.com/VertebrateResequencing/wr's deps
replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0
