package internaltls

import (
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"math/big"
	"time"
)

var clientCert *tls.Config
var serverCert *tls.Config
var certPool *x509.CertPool

func getCert() {
	if clientCert == nil || serverCert == nil {
		serverCert, certPool = generateTestTLSConfig()
		clientCert = &tls.Config{
			RootCAs:    certPool,
			NextProtos: []string{"h3"},
		}
	}
}

func GetTestClientCert() *tls.Config {
	getCert()
	return clientCert
}

func GetTestServerCert() *tls.Config {
	getCert()
	return serverCert
}

func generateTestTLSConfig() (*tls.Config, *x509.CertPool) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{Organization: []string{"Test Co"}},
		NotBefore:    time.Now(),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:     []string{"localhost"},
	}
	certDER, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		panic(err)
	}
	leafCert, err := x509.ParseCertificate(certDER)
	if err != nil {
		panic(err)
	}
	serverTLSConf := &tls.Config{
		Certificates: []tls.Certificate{{Certificate: [][]byte{certDER}, PrivateKey: key, Leaf: leafCert}},
		NextProtos:   []string{"h3"},
	}
	certPool := x509.NewCertPool()
	certPool.AddCert(leafCert)
	return serverTLSConf, certPool
}
