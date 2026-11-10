package middleware

import (
	"encoding/json"
	"net"
	"net/http"
	"sync"
	"time"
)

type rateEntry struct {
	count   int
	expires time.Time
}

type Limiter struct {
	mu       sync.Mutex
	requests map[string]rateEntry
	limit    int
}

func NewLimiter(requestsPerMinute int) *Limiter {
	return &Limiter{requests: make(map[string]rateEntry), limit: requestsPerMinute}
}

func (l *Limiter) Middleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		address, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			address = r.RemoteAddr
		}
		if !l.allow(address, time.Now()) {
			w.Header().Set("Retry-After", "60")
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusTooManyRequests)
			_ = json.NewEncoder(w).Encode(map[string]string{"error": "rate limit exceeded"})
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (l *Limiter) allow(address string, now time.Time) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.requests) > 10000 {
		for key, entry := range l.requests {
			if now.After(entry.expires) {
				delete(l.requests, key)
			}
		}
		if len(l.requests) > 10000 {
			address = "overflow"
		}
	}
	entry := l.requests[address]
	if !now.Before(entry.expires) {
		entry = rateEntry{expires: now.Add(time.Minute)}
	}
	if entry.count >= l.limit {
		return false
	}
	entry.count++
	l.requests[address] = entry
	return true
}
