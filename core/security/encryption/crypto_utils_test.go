package encryption

import (
	"bytes"
	"testing"

	"github.com/stretchr/testify/require"
)

// --- Test Helpers ---

func newCryptoUtilsWithKey(t *testing.T, key []byte) *CryptoUtils {
	t.Helper()
	cu, err := NewCryptoUtils(key)
	require.NoError(t, err)
	require.NotNil(t, cu)
	return cu
}

// --- Tests for NewCryptoUtils ---

func TestNewCryptoUtils_AES128(t *testing.T) {
	key := bytes.Repeat([]byte("a"), 16)
	cu, err := NewCryptoUtils(key)
	require.NoError(t, err)
	require.NotNil(t, cu)
}

func TestNewCryptoUtils_AES192(t *testing.T) {
	key := bytes.Repeat([]byte("b"), 24)
	cu, err := NewCryptoUtils(key)
	require.NoError(t, err)
	require.NotNil(t, cu)
}

func TestNewCryptoUtils_AES256(t *testing.T) {
	key := bytes.Repeat([]byte("c"), 32)
	cu, err := NewCryptoUtils(key)
	require.NoError(t, err)
	require.NotNil(t, cu)
}

func TestNewCryptoUtils_InvalidKeySize(t *testing.T) {
	invalidKeys := [][]byte{
		{},
		bytes.Repeat([]byte("x"), 10),
		bytes.Repeat([]byte("x"), 15),
		bytes.Repeat([]byte("x"), 17),
		bytes.Repeat([]byte("x"), 31),
		bytes.Repeat([]byte("x"), 33),
	}
	for _, key := range invalidKeys {
		_, err := NewCryptoUtils(key)
		require.Error(t, err, "expected error for key of length %d", len(key))
	}
}

// --- Tests for Encrypt and Decrypt ---

func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := []byte("hello, world — this is a test payload")
	ciphertext, err := cu.Encrypt(plaintext)
	require.NoError(t, err)
	require.NotEqual(t, plaintext, ciphertext)

	decrypted, err := cu.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestEncryptDecrypt_EmptyPayload(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 16)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := []byte{}
	ciphertext, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	decrypted, err := cu.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Empty(t, decrypted)
}

func TestEncryptDecrypt_LargePayload(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := bytes.Repeat([]byte("data"), 1024) // 4 KB
	ciphertext, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	decrypted, err := cu.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestEncrypt_ProducesNonDeterministicOutput(t *testing.T) {
	// Two encryptions of the same plaintext should produce different ciphertexts (due to random nonce).
	key := bytes.Repeat([]byte("k"), 32)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := []byte("same plaintext")
	ct1, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	ct2, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	require.NotEqual(t, ct1, ct2, "two encryptions of the same data should differ due to random nonce")
}

func TestDecrypt_TamperedCiphertext(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := []byte("sensitive data")
	ciphertext, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	// Tamper with the ciphertext (flip a bit at the end)
	tampered := make([]byte, len(ciphertext))
	copy(tampered, ciphertext)
	tampered[len(tampered)-1] ^= 0xFF

	_, err = cu.Decrypt(tampered)
	require.Error(t, err, "decryption of tampered ciphertext should fail")
}

func TestDecrypt_CiphertextTooShort(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 32)
	cu := newCryptoUtilsWithKey(t, key)

	// Ciphertext shorter than the nonce size should return an error.
	shortCiphertext := []byte{0x01, 0x02}
	_, err := cu.Decrypt(shortCiphertext)
	require.Error(t, err)
}

func TestEncryptDecrypt_BinaryData(t *testing.T) {
	key := bytes.Repeat([]byte("k"), 24)
	cu := newCryptoUtilsWithKey(t, key)

	plaintext := make([]byte, 256)
	for i := range plaintext {
		plaintext[i] = byte(i)
	}

	ciphertext, err := cu.Encrypt(plaintext)
	require.NoError(t, err)

	decrypted, err := cu.Decrypt(ciphertext)
	require.NoError(t, err)
	require.Equal(t, plaintext, decrypted)
}

func TestEncryptDecrypt_DifferentKeys(t *testing.T) {
	key1 := bytes.Repeat([]byte("a"), 32)
	key2 := bytes.Repeat([]byte("b"), 32)

	cu1 := newCryptoUtilsWithKey(t, key1)
	cu2 := newCryptoUtilsWithKey(t, key2)

	plaintext := []byte("key isolation test")
	ciphertext, err := cu1.Encrypt(plaintext)
	require.NoError(t, err)

	// Decryption with a different key must fail.
	_, err = cu2.Decrypt(ciphertext)
	require.Error(t, err, "decrypting with a wrong key should fail")
}
