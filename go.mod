module github.com/wtsi-hgi/wrstat-ui

go 1.22.0

toolchain go1.22.4

require (
	code.cloudfoundry.org/bytefmt v0.18.0
	github.com/dustin/go-humanize v1.0.1
	github.com/gin-gonic/gin v1.10.0
	github.com/inconshreveable/log15 v2.16.0+incompatible
	github.com/olekukonko/tablewriter v0.0.5
	github.com/smartystreets/goconvey v1.7.2
	github.com/spf13/cobra v1.8.1
	github.com/wtsi-hgi/go-authserver v1.3.0
	github.com/wtsi-ssg/wrstat/v5 v5.3.0
)

require (
	github.com/appleboy/gin-jwt/v2 v2.9.2 // indirect
	github.com/bytedance/sonic v1.11.9 // indirect
	github.com/bytedance/sonic/loader v0.1.1 // indirect
	github.com/cloudwego/base64x v0.1.4 // indirect
	github.com/cloudwego/iasm v0.2.0 // indirect
	github.com/decred/dcrd/dcrec/secp256k1/v4 v4.3.0 // indirect
	github.com/gabriel-vasile/mimetype v1.4.4 // indirect
	github.com/gin-contrib/secure v1.1.0 // indirect
	github.com/gin-contrib/sse v0.1.0 // indirect
	github.com/go-playground/locales v0.14.1 // indirect
	github.com/go-playground/universal-translator v0.18.1 // indirect
	github.com/go-playground/validator/v10 v10.22.0 // indirect
	github.com/go-resty/resty/v2 v2.13.1 // indirect
	github.com/go-stack/stack v1.8.1 // indirect
	github.com/goccy/go-json v0.10.3 // indirect
	github.com/golang-jwt/jwt/v4 v4.5.0 // indirect
	github.com/gopherjs/gopherjs v1.17.2 // indirect
	github.com/gorilla/securecookie v1.1.2 // indirect
	github.com/gorilla/sessions v1.3.0 // indirect
	github.com/hashicorp/errwrap v1.1.0 // indirect
	github.com/hashicorp/go-multierror v1.1.1 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/jtolds/gls v4.20.0+incompatible // indirect
	github.com/klauspost/cpuid/v2 v2.2.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/leodido/go-urn v1.4.0 // indirect
	github.com/lestrrat-go/backoff/v2 v2.0.8 // indirect
	github.com/lestrrat-go/blackmagic v1.0.2 // indirect
	github.com/lestrrat-go/httpcc v1.0.1 // indirect
	github.com/lestrrat-go/iter v1.0.2 // indirect
	github.com/lestrrat-go/jwx v1.2.29 // indirect
	github.com/lestrrat-go/option v1.0.1 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/mattn/go-isatty v0.0.20 // indirect
	github.com/mattn/go-runewidth v0.0.16 // indirect
	github.com/moby/sys/mountinfo v0.7.2 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/okta/okta-jwt-verifier-golang v1.3.1 // indirect
	github.com/patrickmn/go-cache v2.1.0+incompatible // indirect
	github.com/pelletier/go-toml/v2 v2.2.2 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/rivo/uniseg v0.4.7 // indirect
	github.com/rogpeppe/go-internal v1.12.0 // indirect
	github.com/smartystreets/assertions v1.13.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/thanhpk/randstr v1.0.6 // indirect
	github.com/twitchyliquid64/golang-asm v0.15.1 // indirect
	github.com/ugorji/go/codec v1.2.12 // indirect
	github.com/wtsi-ssg/wr v0.5.9 // indirect
	go.etcd.io/bbolt v1.3.11 // indirect
	golang.org/x/arch v0.8.0 // indirect
	golang.org/x/crypto v0.29.0 // indirect
	golang.org/x/exp v0.0.0-20241108190413-2d47ceb2692f // indirect
	golang.org/x/net v0.31.0 // indirect
	golang.org/x/oauth2 v0.24.0 // indirect
	golang.org/x/sync v0.9.0 // indirect
	golang.org/x/sys v0.27.0 // indirect
	golang.org/x/term v0.26.0 // indirect
	golang.org/x/text v0.20.0 // indirect
	golang.org/x/time v0.8.0 // indirect
	google.golang.org/protobuf v1.35.1 // indirect
	gopkg.in/tylerb/graceful.v1 v1.2.15 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

// we need to specify these due to github.com/VertebrateResequencing/wr's deps
replace github.com/grafov/bcast => github.com/grafov/bcast v0.0.0-20161019100130-e9affb593f6c

replace k8s.io/apimachinery => k8s.io/apimachinery v0.0.0-20180228050457-302974c03f7e

replace k8s.io/api => k8s.io/api v0.0.0-20180308224125-73d903622b73

replace k8s.io/client-go => k8s.io/client-go v7.0.0+incompatible

replace github.com/googleapis/gnostic => github.com/googleapis/gnostic v0.4.1-0.20200130232022-81b31a2e6e4e

replace github.com/docker/spdystream => github.com/docker/spdystream v0.1.0

// mergo moved to a vanity URL in v1.0.0
replace github.com/imdario/mergo => github.com/imdario/mergo v0.3.16
