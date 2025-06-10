package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"
)

// CryptoUtils provides utility functions for encryption and decryption.
// It uses AES-GCM (Galois/Counter Mode) for authenticated encryption.
type CryptoUtils struct {
	gcm cipher.AEAD
}

// NewCryptoUtils creates a new CryptoUtils instance.
// The key must be 16, 24, or 32 bytes long to select AES-128, AES-192, or AES-256 respectively.
func NewCryptoUtils(key []byte) (*CryptoUtils, error) {
	// Create a new AES cipher block from the key.
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, fmt.Errorf("failed to create AES cipher: %w", err)
	}

	// Create a new GCM instance, which provides authenticated encryption.
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCM: %w", err)
	}

	return &CryptoUtils{gcm: gcm}, nil
}

// Encrypt encrypts the given plaintext data.
// The nonce is prepended to the ciphertext.
func (c *CryptoUtils) Encrypt(plaintext []byte) ([]byte, error) {
	// Create a new byte slice for the nonce. The size is determined by the GCM implementation.
	nonce := make([]byte, c.gcm.NonceSize())
	// Populate the nonce with a cryptographically secure random sequence.
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, fmt.Errorf("failed to generate nonce: %w", err)
	}

	// Encrypt the data. The gcm.Seal function appends the encrypted data to the first
	// argument and returns the updated slice. We pass nonce as the first argument
	// to prepend it to the ciphertext. The `nil` additional data means we are not
	// using GCM's additional authenticated data feature.
	ciphertext := c.gcm.Seal(nonce, nonce, plaintext, nil)
	return ciphertext, nil
}

// Decrypt decrypts the given ciphertext data.
// It assumes the nonce is prepended to the ciphertext.
func (c *CryptoUtils) Decrypt(ciphertext []byte) ([]byte, error) {
	nonceSize := c.gcm.NonceSize()
	if len(ciphertext) < nonceSize {
		return nil, fmt.Errorf("ciphertext is too short")
	}

	// Extract the nonce from the beginning of the ciphertext.
	nonce, encryptedMessage := ciphertext[:nonceSize], ciphertext[nonceSize:]

	// Decrypt the message. If the authentication tag (which is part of the
	// encryptedMessage) is not valid, this call will return an error.
	plaintext, err := c.gcm.Open(nil, nonce, encryptedMessage, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to decrypt: %w", err)
	}

	return plaintext, nil
}
