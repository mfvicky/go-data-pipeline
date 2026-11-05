package middleware

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"net/http"
)

const APIKeyHeader = "X-API-Key"

func APIKey(secret string) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if secret != "" {
				got := sha256.Sum256([]byte(r.Header.Get(APIKeyHeader)))
				expected := sha256.Sum256([]byte(secret))
				if subtle.ConstantTimeCompare(got[:], expected[:]) != 1 {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusUnauthorized)
					_ = json.NewEncoder(w).Encode(map[string]string{"error": "unauthorized"})
					return
				}
			}
			next.ServeHTTP(w, r)
		})
	}
}
