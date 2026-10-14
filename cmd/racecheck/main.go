// Command racecheck hammers a running control plane with concurrent
// requests and checks that the concurrency rules hold end to end, through
// the real API, outbox, broker, worker and update consumer.
//
// Each round, on a fresh slug:
//
//  1. n concurrent creates with the same slug: exactly one 201, every
//     other 409 tenant_already_exists.
//  2. Wait for the worker to deploy it: active at version 2.
//  3. n concurrent PATCHes with the same version: exactly one 202, every
//     other 409 tenant_version_conflict; then active at version 4.
//  4. n concurrent DELETEs: exactly one 202, every other 409; then
//     destroyed at version 6.
//  5. Exactly three tasks (deploy, update, destroy), all done, and the
//     slug still taken.
//
// It needs the api and a worker with --fail-rate=0 running (see the
// README). It exits 1 if any rule is broken.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

func main() {
	baseURL := flag.String("url", "http://localhost:8080", "control-plane base URL")
	n := flag.Int("n", 32, "concurrent requests per step")
	rounds := flag.Int("rounds", 3, "rounds, each on a fresh tenant")
	timeout := flag.Duration("timeout", 30*time.Second, "how long to wait for the worker to finish a task")
	flag.Parse()
	if *n < 2 || *rounds < 1 {
		fmt.Fprintln(os.Stderr, "racecheck: -n must be at least 2 and -rounds at least 1")
		os.Exit(2)
	}

	c := &client{base: strings.TrimRight(*baseURL, "/"), http: &http.Client{Timeout: 10 * time.Second}, wait: *timeout}
	failed := false
	for r := range *rounds {
		slug := fmt.Sprintf("rc-%s-%d", strconv.FormatInt(time.Now().UnixMilli(), 36), r)
		if err := c.round(context.Background(), slug, *n); err != nil {
			fmt.Printf("round %d (%s): FAIL: %v\n", r+1, slug, err)
			failed = true
			continue
		}
		fmt.Printf("round %d (%s): ok\n", r+1, slug)
	}
	if failed {
		os.Exit(1)
	}
	fmt.Printf("all %d rounds passed with %d concurrent requests per step\n", *rounds, *n)
}

type tenant struct {
	ID      string `json:"id"`
	Status  string `json:"status"`
	Version int64  `json:"version"`
}

type task struct {
	Type   string `json:"type"`
	Status string `json:"status"`
}

type client struct {
	base string
	http *http.Client
	wait time.Duration
}

type response struct {
	status int
	body   []byte
}

// key is how results are tallied: the status, plus the error code for
// errors, e.g. "409 tenant_version_conflict".
func (r response) key() string {
	k := strconv.Itoa(r.status)
	if r.status >= 400 {
		var e struct {
			Error struct {
				Code string `json:"code"`
			} `json:"error"`
		}
		_ = json.Unmarshal(r.body, &e)
		k += " " + e.Error.Code
	}
	return k
}

func (c *client) do(ctx context.Context, method, path string, body any) (response, error) {
	var r io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return response{}, err
		}
		r = bytes.NewReader(b)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, r)
	if err != nil {
		return response{}, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	res, err := c.http.Do(req)
	if err != nil {
		return response{}, err
	}
	defer func() { _ = res.Body.Close() }()
	b, err := io.ReadAll(res.Body)
	return response{status: res.StatusCode, body: b}, err
}

// concurrently sends n requests released at the same moment and tallies
// the results. The winning response body is returned for inspection.
func (c *client) concurrently(ctx context.Context, n int, method, path string, body func(i int) any) (map[string]int, []byte, error) {
	var (
		wg      sync.WaitGroup
		mu      sync.Mutex
		tally   = map[string]int{}
		winner  []byte
		errs    []error
		release = make(chan struct{})
	)
	for i := range n {
		wg.Go(func() {
			<-release
			var b any
			if body != nil {
				b = body(i)
			}
			res, err := c.do(ctx, method, path, b)
			mu.Lock()
			defer mu.Unlock()
			if err != nil {
				errs = append(errs, err)
				return
			}
			tally[res.key()]++
			if res.status < 300 {
				winner = res.body
			}
		})
	}
	close(release)
	wg.Wait()
	return tally, winner, errors.Join(errs...)
}

// expectOneWinner checks that exactly one request got winStatus and that
// every other one got one of the allowed error keys.
func expectOneWinner(step string, tally map[string]int, n int, winStatus string, losers ...string) error {
	if tally[winStatus] != 1 {
		return fmt.Errorf("%s: %d × %s, want exactly 1 (results %s)", step, tally[winStatus], winStatus, format(tally))
	}
	lost := 0
	for _, k := range losers {
		lost += tally[k]
	}
	if lost != n-1 {
		return fmt.Errorf("%s: want the other %d to get %s (results %s)", step, n-1, strings.Join(losers, " or "), format(tally))
	}
	return nil
}

func format(tally map[string]int) string {
	keys := make([]string, 0, len(tally))
	for k := range tally {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = fmt.Sprintf("%d × %s", tally[k], k)
	}
	return "[" + strings.Join(parts, ", ") + "]"
}

func (c *client) getTenant(ctx context.Context, id string) (tenant, error) {
	res, err := c.do(ctx, http.MethodGet, "/v1/tenants/"+id, nil)
	if err != nil {
		return tenant{}, err
	}
	if res.status != http.StatusOK {
		return tenant{}, fmt.Errorf("GET tenant %s: %d %s", id, res.status, res.body)
	}
	var t tenant
	return t, json.Unmarshal(res.body, &t)
}

// waitFor polls the tenant until it reaches status, and checks its version.
func (c *client) waitFor(ctx context.Context, id, status string, version int64) error {
	deadline := time.Now().Add(c.wait)
	for {
		t, err := c.getTenant(ctx, id)
		if err != nil {
			return err
		}
		switch {
		case t.Status == status && t.Version == version:
			return nil
		case t.Status == status || t.Status == "failed":
			return fmt.Errorf("tenant is %s v%d, want %s v%d (is the worker running with --fail-rate=0?)", t.Status, t.Version, status, version)
		case time.Now().After(deadline):
			return fmt.Errorf("tenant still %s v%d after %v, want %s v%d (is the worker running?)", t.Status, t.Version, c.wait, status, version)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *client) round(ctx context.Context, slug string, n int) error {
	// 1. Same-slug creates.
	tally, body, err := c.concurrently(ctx, n, http.MethodPost, "/v1/tenants", func(int) any {
		return map[string]any{"slug": slug, "name": "Race check"}
	})
	if err != nil {
		return err
	}
	if err := expectOneWinner("create", tally, n, "201", "409 tenant_already_exists"); err != nil {
		return err
	}
	var created struct {
		Tenant tenant `json:"tenant"`
	}
	if err := json.Unmarshal(body, &created); err != nil {
		return err
	}
	id := created.Tenant.ID
	if err := c.waitFor(ctx, id, "active", 2); err != nil {
		return fmt.Errorf("deploy: %w", err)
	}

	// 2. Same-version PATCHes.
	tally, _, err = c.concurrently(ctx, n, http.MethodPatch, "/v1/tenants/"+id, func(i int) any {
		return map[string]any{"name": fmt.Sprintf("Race check %d", i), "version": 2}
	})
	if err != nil {
		return err
	}
	if err := expectOneWinner("patch", tally, n, "202", "409 tenant_version_conflict"); err != nil {
		return err
	}
	if err := c.waitFor(ctx, id, "active", 4); err != nil {
		return fmt.Errorf("update: %w", err)
	}

	// 3. Concurrent DELETEs. DELETE carries no version: a loser that read
	// the tenant before the winner committed gets a version conflict, one
	// that read after gets update_not_allowed (the tenant is destroying).
	tally, _, err = c.concurrently(ctx, n, http.MethodDelete, "/v1/tenants/"+id, nil)
	if err != nil {
		return err
	}
	if err := expectOneWinner("delete", tally, n, "202", "409 tenant_version_conflict", "409 tenant_update_not_allowed"); err != nil {
		return err
	}
	if err := c.waitFor(ctx, id, "destroyed", 6); err != nil {
		return fmt.Errorf("destroy: %w", err)
	}

	// 4. Exactly the three winning tasks exist, all done; the slug is
	// still taken.
	res, err := c.do(ctx, http.MethodGet, "/v1/tasks?tenant_id="+id, nil)
	if err != nil {
		return err
	}
	var tasks struct {
		Items []task `json:"items"`
	}
	if err := json.Unmarshal(res.body, &tasks); err != nil {
		return fmt.Errorf("list tasks: %d %s: %w", res.status, res.body, err)
	}
	got := make([]string, len(tasks.Items))
	for i, t := range tasks.Items {
		got[i] = t.Type + ":" + t.Status
	}
	if want := "deploy:done update:done destroy:done"; strings.Join(got, " ") != want {
		return fmt.Errorf("tasks = %v, want [%s]", got, want)
	}
	res, err = c.do(ctx, http.MethodPost, "/v1/tenants", map[string]any{"slug": slug, "name": "Again"})
	if err != nil {
		return err
	}
	if k := res.key(); k != "409 tenant_already_exists" {
		return fmt.Errorf("re-create destroyed slug: %s, want 409 tenant_already_exists", k)
	}
	return nil
}
