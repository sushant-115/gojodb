package certs

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"log"
	"math/big"
	"net"
	"os"
	"time"
)

func withWorkingDir(dir string, fn func(s, t, v string) (*tls.Config, error)) (func(s, t, v string) (*tls.Config, error), error) {
	// Save current directory
	oldDir, err := os.Getwd()
	if err != nil {
		return nil, err
	}

	// Change to new directory
	if err := os.Chdir(dir); err != nil {
		return nil, err
	}

	// Ensure we always go back, even if fn panics or returns error
	defer func() {
		_ = os.Chdir(oldDir)
	}()

	// Run the function in new directory
	return fn, nil
}

// main function orchestrates the generation of certs and demonstrates loading them.
func LoadCerts(dir string) (*tls.Config, *tls.Config) {
	// Load the server's TLS configuration.
	fn, err := withWorkingDir(dir, LoadServerTLSConfig)
	if err != nil {
		log.Fatalf("Failed to load server TLS config: %v", err)
	}
	serverCert, err := fn("/etc/gojodb/certs/ca.crt", "/etc/gojodb/certs/server.crt", "/etc/gojodb/certs/server.key")
	if err != nil {
		log.Fatalf("Failed to load server TLS config: %v", err)
	}

	cfn, err := withWorkingDir(dir, LoadClientTLSConfig)
	if err != nil {
		log.Fatalf("Failed to load server TLS config: %v", err)
	}
	clientCert, err := cfn("/etc/gojodb/certs/ca.crt", "/etc/gojodb/certs/client.crt", "/etc/gojodb/certs/client.key")
	if err != nil {
		log.Fatalf("Failed to load server TLS config: %v", err)
	}

	return serverCert, clientCert
}

// loadServerTLSConfig loads the server's certificate and key, and the CA cert.
// It configures the server to require and verify client certificates.
func LoadServerTLSConfig(caCertPath, serverCertPath, serverKeyPath string) (*tls.Config, error) {
	// Load the server's certificate and private key.
	serverCert, err := tls.LoadX509KeyPair(serverCertPath, serverKeyPath)
	if err != nil {
		return nil, fmt.Errorf("could not load server key pair: %w", err)
	}

	// Create a certificate pool from the CA certificate.
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("could not read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert to pool")
	}

	// Create and return the TLS config.
	return &tls.Config{
		Certificates: []tls.Certificate{serverCert},
		ClientAuth:   tls.RequireAndVerifyClientCert, // Require clients to present a certificate.
		ClientCAs:    caCertPool,                     // Use this CA to verify the client's certificate.
		NextProtos:   []string{"h3"},
		// --- START DIAGNOSTIC HOOK ---
		// This function will run for every incoming connection attempt.
		// --- START DIAGNOSTIC HOOK ---
		// This function runs AFTER the client has presented its certificates.
		// This is the correct place to debug mTLS issues.
		VerifyPeerCertificate: func(rawCerts [][]byte, verifiedChains [][]*x509.Certificate) error {
			log.Println("--- TLS HANDSHAKE DIAGNOSTICS (SERVER-SIDE) ---")
			log.Printf("Hook 'VerifyPeerCertificate' was triggered by: %s\n", "a connecting client")

			if len(rawCerts) == 0 {
				log.Println("❌ FATAL: Client did not present any certificates.")
			} else {
				log.Printf("✅ Client presented %d certificate(s).\n", len(rawCerts))
				// Attempt to parse the first certificate to see what it is.
				cert, err := x509.ParseCertificate(rawCerts[0])
				if err != nil {
					log.Printf("   - Error parsing client certificate: %v\n", err)
				} else {
					log.Printf("   - Client Certificate Subject: %s\n", cert.Subject)
					log.Printf("   - Client Certificate Issuer: %s\n", cert.Issuer)
				}
			}

			if len(verifiedChains) > 0 {
				log.Println("✅ Certificate chain was successfully verified by the standard library.")
			} else {
				log.Println("❌ FATAL: Certificate chain could NOT be verified against the server's ClientCAs pool.")
			}
			log.Println("-----------------------------------------------")

			// We are only using this for logging. We return nil to let the default
			// verification logic proceed. The connection will still fail if the
			// standard library's own check fails.
			return nil
		},
	}, nil
}

// loadClientTLSConfig loads the client's certificate and key, and the CA cert.
// It configures the client to present its certificate to the server and verify the server's cert.
func LoadClientTLSConfig(caCertPath, clientCertPath, clientKeyPath string) (*tls.Config, error) {
	// Load the client's certificate and private key.
	clientCert, err := tls.LoadX509KeyPair(clientCertPath, clientKeyPath)
	if err != nil {
		return nil, fmt.Errorf("could not load client key pair: %w", err)
	}

	// Create a certificate pool from the CA certificate.
	caCert, err := os.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("could not read CA certificate: %w", err)
	}
	caCertPool := x509.NewCertPool()
	if !caCertPool.AppendCertsFromPEM(caCert) {
		return nil, fmt.Errorf("failed to append CA cert to pool")
	}

	// Create and return the TLS config.
	return &tls.Config{
		InsecureSkipVerify: true,
		Certificates:       []tls.Certificate{clientCert},
		RootCAs:            caCertPool, // Use this CA to verify the server's certificate.
		NextProtos:         []string{"h3"},
	}, nil
}

// --- Certificate Generation Helper Functions ---

// generateCerts creates all necessary certificates and keys.
func generateCerts() error {
	// 1. Generate CA private key and certificate.
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	caCert, err := createCACertificate(caKey)
	if err != nil {
		return err
	}
	if err := saveCert("ca.crt", caCert); err != nil {
		return err
	}
	if err := saveKey("ca.key", caKey); err != nil {
		return err
	}

	// 2. Generate Server private key and certificate, signed by CA.
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	serverCert, err := createSignedCertificate(serverKey, "localhost", caCert, caKey, true)
	if err != nil {
		return err
	}
	if err := saveCert("server.crt", serverCert); err != nil {
		return err
	}
	if err := saveKey("server.key", serverKey); err != nil {
		return err
	}

	// 3. Generate Client private key and certificate, signed by CA.
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return err
	}
	clientCert, err := createSignedCertificate(clientKey, "client", caCert, caKey, false)
	if err != nil {
		return err
	}
	if err := saveCert("client.crt", clientCert); err != nil {
		return err
	}
	if err := saveKey("client.key", clientKey); err != nil {
		return err
	}

	return nil
}

// createCACertificate creates a self-signed CA certificate.
func createCACertificate(privateKey *ecdsa.PrivateKey) (*x509.Certificate, error) {
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"My CA"},
		},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().AddDate(1, 0, 0),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		BasicConstraintsValid: true,
	}
	certBytes, err := x509.CreateCertificate(rand.Reader, template, template, &privateKey.PublicKey, privateKey)
	if err != nil {
		return nil, err
	}
	return x509.ParseCertificate(certBytes)
}

// createSignedCertificate creates a server or client cert signed by a CA.
func createSignedCertificate(
	privateKey *ecdsa.PrivateKey,
	commonName string,
	caCert *x509.Certificate,
	caKey *ecdsa.PrivateKey,
	isServer bool,
) (*x509.Certificate, error) {

	serialNumber, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("failed to generate serial: %w", err)
	}

	// Base template
	template := &x509.Certificate{
		SerialNumber: serialNumber,
		Subject: pkix.Name{
			CommonName: commonName,
		},
		NotBefore: time.Now(),
		NotAfter:  time.Now().AddDate(1, 0, 0), // valid for 1 year
		KeyUsage:  x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
	}

	// SANs (must be set or Go rejects certs)
	template.DNSNames = []string{commonName}
	if commonName == "localhost" {
		template.IPAddresses = []net.IP{net.ParseIP("127.0.0.1")}
	}

	// Usage
	if isServer {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}
	} else {
		template.ExtKeyUsage = []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}
	}

	// Sign the cert
	certBytes, err := x509.CreateCertificate(rand.Reader, template, caCert, &privateKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("create cert: %w", err)
	}

	return x509.ParseCertificate(certBytes)
}

// saveCert saves a certificate to a PEM file.
func saveCert(filename string, cert *x509.Certificate) error {
	certOut, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer certOut.Close()
	return pem.Encode(certOut, &pem.Block{Type: "CERTIFICATE", Bytes: cert.Raw})
}

// saveKey saves a private key to a PEM file.
func saveKey(filename string, key *ecdsa.PrivateKey) error {
	keyOut, err := os.OpenFile(filename, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer keyOut.Close()
	keyBytes, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return err
	}
	return pem.Encode(keyOut, &pem.Block{Type: "EC PRIVATE KEY", Bytes: keyBytes})
}
