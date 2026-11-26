/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package redis

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	goredis "github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func TestFactoryUsesIndependentStateAndCoordinationTTLs(t *testing.T) {
	server := miniredis.RunT(t)
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(fmt.Sprintf(
		`{"address": %q, "stateTTL": "750ms", "coordinationTTL": "3m"}`,
		server.Addr(),
	))), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })

	require.Equal(t, 750*time.Millisecond, syncer.stateTTL)
	require.Equal(t, 3*time.Minute, syncer.coordinationTTL)
}

func TestFactoryReadsPasswordFromEnvironment(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("TEST_REDIS_PASSWORD", "environment-secret")
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(fmt.Sprintf(
		`{"address": %q, "passwordEnv": "TEST_REDIS_PASSWORD"}`,
		server.Addr(),
	))), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })

	require.Equal(t, "environment-secret", syncer.client.(*goredis.Client).Options().Password)
}

func TestFactoryReadsPasswordFromFile(t *testing.T) {
	for _, tc := range []struct {
		name     string
		contents string
		password string
	}{
		{name: "no line ending", contents: "file-secret", password: "file-secret"},
		{name: "LF", contents: "file-secret\n", password: "file-secret"},
		{name: "CRLF", contents: "file-secret\r\n", password: "file-secret"},
		{name: "multiple line endings", contents: "file-secret\r\n\n", password: "file-secret"},
		{name: "preserve other whitespace", contents: " \tfile-secret \t\r\n", password: " \tfile-secret \t"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := miniredis.RunT(t)
			server.RequireAuth(tc.password)
			passwordPath := filepath.Join(t.TempDir(), "password")
			require.NoError(t, os.WriteFile(passwordPath, []byte(tc.contents), 0o600))
			plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(fmt.Sprintf(
				`{"address": %q, "passwordFile": %q}`,
				server.Addr(), passwordPath,
			))), nil)
			require.NoError(t, err)
			syncer := plugin.(*RedisSyncer)
			t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })

			require.Equal(t, tc.password, syncer.client.(*goredis.Client).Options().Password)
			require.NoError(t, syncer.client.Ping(t.Context()).Err())
		})
	}
}

func TestFactoryRejectsUnsafeOrAmbiguousPasswordConfiguration(t *testing.T) {
	server := miniredis.RunT(t)
	t.Setenv("TEST_REDIS_PASSWORD", "secret")
	passwordPath := filepath.Join(t.TempDir(), "password")
	require.NoError(t, os.WriteFile(passwordPath, []byte("secret"), 0o600))

	testCases := []struct {
		name    string
		params  string
		wantErr string
	}{
		{
			name:    "plaintext password",
			params:  fmt.Sprintf(`{"address": %q, "password": "secret"}`, server.Addr()),
			wantErr: "passwordEnv or passwordFile",
		},
		{
			name: "environment and file",
			params: fmt.Sprintf(
				`{"address": %q, "passwordEnv": "TEST_REDIS_PASSWORD", "passwordFile": %q}`,
				server.Addr(), passwordPath,
			),
			wantErr: "mutually exclusive",
		},
		{
			name:    "missing environment variable",
			params:  fmt.Sprintf(`{"address": %q, "passwordEnv": "MISSING_REDIS_PASSWORD"}`, server.Addr()),
			wantErr: "MISSING_REDIS_PASSWORD",
		},
		{
			name:    "missing file",
			params:  fmt.Sprintf(`{"address": %q, "passwordFile": %q}`, server.Addr(), filepath.Join(t.TempDir(), "missing")),
			wantErr: "password file",
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(tc.params)), nil)
			require.ErrorContains(t, err, tc.wantErr)
			require.Nil(t, plugin)
		})
	}
}

func TestFactoryRejectsACLUsername(t *testing.T) {
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(`{"username": "router"}`)), nil)
	if plugin != nil {
		t.Cleanup(func() { require.NoError(t, plugin.(*RedisSyncer).client.Close()) })
	}
	require.ErrorContains(t, err, "unknown field")
	require.Nil(t, plugin)
}

func TestFactoryWithoutTLSUsesPlaintext(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q}`, server.Addr()))

	require.Nil(t, syncer.client.(*goredis.Client).Options().TLSConfig)
}

func TestFactoryBuildsTLSClient(t *testing.T) {
	server := miniredis.RunT(t)
	syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "tls": {"serverName": "redis.example"}}`, server.Addr()))

	client, ok := syncer.client.(*goredis.Client)
	require.True(t, ok, "client type %T", syncer.client)
	tlsConfig := client.Options().TLSConfig
	require.NotNil(t, tlsConfig)
	require.Equal(t, uint16(tls.VersionTLS12), tlsConfig.MinVersion)
	require.Equal(t, "redis.example", tlsConfig.ServerName)
	require.Nil(t, tlsConfig.RootCAs, "without caFile the system roots are used")
}

func TestFactoryTLSConnectsWithCAFile(t *testing.T) {
	caFile, serverTLS := newTLSFixture(t)
	server, err := miniredis.RunTLS(serverTLS)
	require.NoError(t, err)
	t.Cleanup(server.Close)

	trusted := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "tls": {"caFile": %q}}`, server.Addr(), caFile))
	require.NoError(t, trusted.client.Ping(t.Context()).Err())

	untrusted := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "tls": {}}`, server.Addr()))
	require.ErrorContains(t, untrusted.client.Ping(t.Context()).Err(), "certificate")
}

func TestFactoryRejectsBadTLSConfig(t *testing.T) {
	noCertificates := filepath.Join(t.TempDir(), "empty.pem")
	require.NoError(t, os.WriteFile(noCertificates, []byte("not a certificate\n"), 0o600))

	for _, tc := range []struct {
		name    string
		params  string
		wantErr string
	}{
		{
			name:    "missing caFile",
			params:  fmt.Sprintf(`{"tls": {"caFile": %q}}`, filepath.Join(t.TempDir(), "missing.pem")),
			wantErr: "read tls caFile",
		},
		{
			name:    "caFile without certificates",
			params:  fmt.Sprintf(`{"tls": {"caFile": %q}}`, noCertificates),
			wantErr: "no certificates",
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(tc.params)), nil)
			require.ErrorContains(t, err, tc.wantErr)
			require.Nil(t, plugin)
		})
	}
}

func TestFactoryBuildsClusterClient(t *testing.T) {
	t.Run("options", func(t *testing.T) {
		server := miniredis.RunT(t)
		syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "cluster": true, "tls": {"serverName": "redis.example"}}`, server.Addr()))

		client, ok := syncer.client.(*goredis.ClusterClient)
		require.True(t, ok, "client type %T", syncer.client)
		require.Equal(t, []string{server.Addr()}, client.Options().Addrs)
		require.NotNil(t, client.Options().TLSConfig)
		require.Equal(t, "redis.example", client.Options().TLSConfig.ServerName)
	})

	t.Run("round trip", func(t *testing.T) {
		server := miniredis.RunT(t)
		syncer := newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "cluster": true}`, server.Addr()))

		actual, existed, err := syncer.GetOrSet(t.Context(), "request", "request-id", "first")
		require.NoError(t, err)
		require.False(t, existed)
		require.Equal(t, "first", actual)

		actual, existed, err = syncer.GetOrSet(t.Context(), "request", "request-id", "second")
		require.NoError(t, err)
		require.True(t, existed)
		require.Equal(t, "first", actual)
	})
}

func TestFactoryRejectsDBInClusterMode(t *testing.T) {
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(`{"cluster": true, "db": 1}`)), nil)
	require.ErrorContains(t, err, "db must be 0 in cluster mode")
	require.Nil(t, plugin)
}

func TestFactoryConnectsBeforeTheFirstCall(t *testing.T) {
	for _, cluster := range []bool{false, true} {
		t.Run(fmt.Sprintf("cluster=%t", cluster), func(t *testing.T) {
			server := miniredis.RunT(t)
			newSyncerFromParams(t, fmt.Sprintf(`{"address": %q, "cluster": %t}`, server.Addr(), cluster))

			require.Eventually(t, func() bool { return server.CurrentConnectionCount() >= idleConnections },
				5*time.Second, 10*time.Millisecond, "connections open before the first call")
		})
	}
}

func newSyncerFromParams(t *testing.T, params string) *RedisSyncer {
	t.Helper()
	plugin, err := RedisSyncerFactory("redis", json.NewDecoder(strings.NewReader(params)), nil)
	require.NoError(t, err)
	syncer := plugin.(*RedisSyncer)
	t.Cleanup(func() { require.NoError(t, syncer.client.Close()) })
	return syncer
}

// newTLSFixture writes a self-signed CA to a file and returns its path with a
// server TLS config whose certificate that CA signed for 127.0.0.1.
func newTLSFixture(t *testing.T) (string, *tls.Config) {
	t.Helper()
	notBefore, notAfter := time.Now().Add(-time.Hour), time.Now().Add(time.Hour)

	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "redis-syncer test CA"},
		NotBefore:             notBefore,
		NotAfter:              notAfter,
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	require.NoError(t, err)
	caCert, err := x509.ParseCertificate(caDER)
	require.NoError(t, err)

	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	require.NoError(t, err)
	serverTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "127.0.0.1"},
		NotBefore:    notBefore,
		NotAfter:     notAfter,
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
	}
	serverDER, err := x509.CreateCertificate(rand.Reader, serverTemplate, caCert, &serverKey.PublicKey, caKey)
	require.NoError(t, err)

	caFile := filepath.Join(t.TempDir(), "ca.pem")
	require.NoError(t, os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}), 0o600))
	return caFile, &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{serverDER}, PrivateKey: serverKey}},
		MinVersion:   tls.VersionTLS12,
	}
}
