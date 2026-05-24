/*******************************************************************************
 * Copyright (c) 2026 Genome Research Ltd.
 *
 * Author: Sendu Bala <sb10@sanger.ac.uk>
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

import (
	"context"
	"crypto/tls"
	"net"
	"time"

	ch "github.com/ClickHouse/clickhouse-go/v2"
)

const maintenanceReadTimeout = 100 * 365 * 24 * time.Hour

const defaultClickHouseDriverDialTimeout = 30 * time.Second

type clickHouseConfigConnector func(context.Context, Config) (ch.Conn, error)

type noReadDeadlineConn struct {
	net.Conn
}

func (c noReadDeadlineConn) SetReadDeadline(time.Time) error {
	return nil
}

func noReadDeadlineDialContext(
	dial func(context.Context, string) (net.Conn, error),
) func(context.Context, string) (net.Conn, error) {
	return func(ctx context.Context, addr string) (net.Conn, error) {
		conn, err := dial(ctx, addr)
		if err != nil {
			return nil, err
		}

		return noReadDeadlineConn{Conn: conn}, nil
	}
}

func connectMaintenanceFromConfig(ctx context.Context, cfg Config) (ch.Conn, error) {
	opts, err := maintenanceOptionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	return connectAndBootstrap(ctx, opts, cfg.Database, queryTimeout(cfg))
}

func maintenanceOptionsFromConfig(cfg Config) (*ch.Options, error) {
	opts, err := optionsFromConfig(cfg)
	if err != nil {
		return nil, err
	}

	opts.ReadTimeout = maintenanceReadTimeout
	opts.DialContext = noReadDeadlineDialContext(defaultDialContextForOptions(opts))

	return opts, nil
}

func defaultDialContextForOptions(opts *ch.Options) func(context.Context, string) (net.Conn, error) {
	if opts.DialContext != nil {
		return opts.DialContext
	}

	dialTimeout := opts.DialTimeout
	if dialTimeout <= 0 {
		dialTimeout = defaultClickHouseDriverDialTimeout
	}

	tlsConfig := opts.TLS

	return func(ctx context.Context, addr string) (net.Conn, error) {
		dialer := &net.Dialer{Timeout: dialTimeout}
		if tlsConfig == nil {
			return dialer.DialContext(ctx, "tcp", addr)
		}

		return tlsDialContext(ctx, dialer, tlsConfig, addr)
	}
}

func tlsDialContext(
	ctx context.Context,
	dialer *net.Dialer,
	tlsConfig *tls.Config,
	addr string,
) (net.Conn, error) {
	tlsDialer := &tls.Dialer{
		NetDialer: dialer,
		Config:    tlsConfig,
	}

	return tlsDialer.DialContext(ctx, "tcp", addr)
}
