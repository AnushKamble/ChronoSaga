package workflow

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// CanonicalJSON normalizes any JSON-serializable value into deterministic bytes with sorted keys.
func CanonicalJSON(val any) ([]byte, error) {
	if val == nil {
		return []byte("null"), nil
	}

	raw, err := json.Marshal(val)
	if err != nil {
		return nil, fmt.Errorf("failed marshaling value: %w", err)
	}

	var parsed any
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("failed unmarshaling value for normalization: %w", err)
	}

	normalized := normalizeValue(parsed)
	return json.Marshal(normalized)
}

func normalizeValue(v any) any {
	switch val := v.(type) {
	case map[string]any:
		res := make(map[string]any, len(val))
		for k, child := range val {
			res[k] = normalizeValue(child)
		}
		return res
	case []any:
		res := make([]any, len(val))
		for i, child := range val {
			res[i] = normalizeValue(child)
		}
		return res
	default:
		return val
	}
}

// CanonicalHash generates a deterministic SHA-256 hex string for a command's components.
func CanonicalHash(kind, name string, input any, options any) (string, error) {
	inputBytes, err := CanonicalJSON(input)
	if err != nil {
		return "", fmt.Errorf("failed normalizing input: %w", err)
	}

	optsBytes, err := CanonicalJSON(options)
	if err != nil {
		return "", fmt.Errorf("failed normalizing options: %w", err)
	}

	h := sha256.New()
	h.Write([]byte(kind))
	h.Write([]byte(":"))
	h.Write([]byte(name))
	h.Write([]byte(":"))
	h.Write(inputBytes)
	h.Write([]byte(":"))
	h.Write(optsBytes)

	return hex.EncodeToString(h.Sum(nil)), nil
}
