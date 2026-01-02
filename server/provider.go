package server

import "github.com/wtsi-hgi/wrstat-ui/provider"

const ErrNoPaths = provider.ErrNoPaths

// Re-export provider contracts to satisfy interface_spec while avoiding import cycles.
type Error = provider.Error

type Provider = provider.Provider
