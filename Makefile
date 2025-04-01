PKG := github.com/wtsi-hgi/wrstat-ui
VERSION := $(shell git describe --tags --always --long --dirty)
TAG := $(shell git describe --abbrev=0 --tags)
LDFLAGS = -ldflags "-X ${PKG}/cmd.Version=${VERSION}"
export GOPATH := $(shell go env GOPATH)
PATH := $(PATH):${GOPATH}/bin

default: install

# We require CGO_ENABLED=1 for getting group information to work properly; the
# pure go version doesn't work on all systems such as those using LDAP for
# groups

build: export CGO_ENABLED = 1
build:
	@cd server/static/wrstat; npm install && npm run build:prod
	go build -tags netgo ${LDFLAGS}

buildembedded:
	@cd analytics; ./embed.sh;
	@cd syscalls; ./embed.sh;

buildnonpm: export CGO_ENABLED = 1
buildnonpm:
	go build -tags netgo ${LDFLAGS}

install: export CGO_ENABLED = 1
install:
	@rm -f ${GOPATH}/bin/wrstat-ui
	@cd server/static/wrstat; npm install && npm run build:prod
	@echo Starting go install
	@go install -tags netgo ${LDFLAGS}
	@echo Installed to ${GOPATH}/bin/wrstat-ui

installnonpm: export CGO_ENABLED = 1
installnonpm:
	@rm -f ${GOPATH}/bin/wrstat-ui
	go install -tags netgo ${LDFLAGS}
	@echo installed to ${GOPATH}/bin/wrstat-ui

test: export CGO_ENABLED = 1
test:
	@cd server/static/wrstat; npm install && CI= npm run build:prod
	@go test -tags netgo --count 1 ./...
	@cd server/static/wrstat; CI=1 npm test

race: export CGO_ENABLED = 1
race:
	go test -tags netgo -race --count 1 ./...

bench: export CGO_ENABLED = 1
bench:
	go test -tags netgo --count 1 -run Bench -bench=. ./...

# curl -sSfL https://raw.githubusercontent.com/golangci/golangci-lint/master/install.sh | sh -s -- -b $(go env GOPATH)/bin v1.59.1
lint: export CGO_ENABLED = 1
lint:
	@cd server/static/wrstat; npm install && CI= npm run lint || true 
	@golangci-lint run --timeout 2m
# remove the || true when you get round to removing all anys

lintnonpm: export CGO_ENABLED = 1
lintnonpm:
	@golangci-lint run --timeout 2m

clean:
	@rm -f ./wrstat-ui
	@rm -f ./dist.zip

dist: export CGO_ENABLED = 1
# go get -u github.com/gobuild/gopack
# go get -u github.com/aktau/github-release
dist:
	gopack pack --os linux --arch amd64 -o linux-dist.zip
	github-release release --tag ${TAG} --pre-release
	github-release upload --tag ${TAG} --name wrstat-ui-linux-x86-64.zip --file linux-dist.zip
	@rm -f wrstat-ui linux-dist.zip

.PHONY: test race bench lint build install clean dist
