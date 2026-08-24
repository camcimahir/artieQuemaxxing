// Command client is a standalone demonstration that drives a running
// Queuemaxxing server through the Frankenstein scheduling algorithm
// (ARCHITECTURE.md §4): delay visibility, highest-priority-first, and
// FIFO tie-breaking by Seq.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"queuemaxxing/pkg/model"
)

const (
	pollInterval = 500 * time.Millisecond
	pollWindow   = 6 * time.Second
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", getenv("QUEUE_URL", "http://127.0.0.1:8080"), "base URL of the running Queuemaxxing server (env: QUEUE_URL)")
	flag.Parse()
	base := strings.TrimRight(strings.TrimSpace(*url), "/")

	httpClient := &http.Client{Timeout: 5 * time.Second}

	printBanner()

	if err := checkHealth(httpClient, base); err != nil {
		return fmt.Errorf("%w\nStart the server first:  go run ./cmd/server", err)
	}
	st, err := getStats(httpClient, base)
	if err != nil {
		return err
	}
	if st.Total != 0 {
		return fmt.Errorf("queue is not empty (%d total, %d ready, %d delayed); the demo needs a fresh WAL so the scripted order is observable", st.Total, st.Ready, st.Delayed)
	}
	fmt.Printf("Connected to %s  (queue empty, ready to enqueue)\n\n", base)

	script := []task{
		{Label: "A", Priority: 1, DelaySeconds: 0, Payload: "Task A (Low Pri, Immediate)"},
		{Label: "B", Priority: 5, DelaySeconds: 0, Payload: "Task B (Medium Pri, Immediate)"},
		{Label: "C", Priority: 10, DelaySeconds: 4, Payload: "Task C (High Pri, 4s Delay)"},
		{Label: "D", Priority: 5, DelaySeconds: 0, Payload: "Task D (Medium Pri, Immediate - Tie Check)"},
		{Label: "E", Priority: 10, DelaySeconds: 2, Payload: "Task E (High Pri, 2s Delay)"},
	}

	fmt.Println(bold("── 1. Enqueue ─────────────────────────────────────────────────────"))
	pending := make([]*tracked, 0, len(script))
	for _, t := range script {
		msg, err := enqueue(httpClient, base, model.EnqueueRequest{
			Payload:      t.Payload,
			Priority:     t.Priority,
			DelaySeconds: t.DelaySeconds,
		})
		if err != nil {
			return fmt.Errorf("enqueue %s: %w", t.Label, err)
		}
		tr := &tracked{task: t, msg: msg}
		pending = append(pending, tr)
		fmt.Printf("  %s  seq=%-4d pri=%-3d delay=%ds  %s\n",
			bold(t.Label), msg.Seq, t.Priority, t.DelaySeconds, availability(t))
	}

	expected := []string{"B", "D", "A", "E", "C"}
	fmt.Printf("\nExpected FIFO dequeue order: %s\n\n", strings.Join(expected, " → "))

	fmt.Println(bold("── 2. Poll POST /dequeue every 500ms for 6s ───────────────────────"))
	pollStart := time.Now()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	timeout := time.NewTimer(pollWindow)
	defer timeout.Stop()

	var actual []*tracked
	if err := pollOnce(httpClient, base, pollStart, &pending, &actual); err != nil {
		return err
	}
	for {
		select {
		case <-timeout.C:
			fmt.Println()
			printSummary(expected, actual)
			if !orderMatches(expected, actual) {
				return fmt.Errorf("dequeue order did not match expectation (see table above)")
			}
			return nil
		case <-ticker.C:
			if err := pollOnce(httpClient, base, pollStart, &pending, &actual); err != nil {
				return err
			}
		}
	}
}

type task struct {
	Label        string
	Priority     int
	DelaySeconds int64
	Payload      string
}

type tracked struct {
	task
	msg model.Message
}

type statsResponse struct {
	Total   int `json:"total"`
	Ready   int `json:"ready"`
	Delayed int `json:"delayed"`
}

type healthResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func printBanner() {
	fmt.Print(`
╔══════════════════════════════════════════════════════════════════════╗
║          QUEUMAXXING — Frankenstein Capability Demonstration         ║
╚══════════════════════════════════════════════════════════════════════╝

The in-memory engine (FrankensteinQueue) selects one message per dequeue:

  1. Visibility filter  delayed messages (AvailableAt > now) are invisible
  2. Priority           among visible PENDING messages, highest Priority wins
  3. FIFO tie-break     equal Priority → smallest Seq (earliest enqueue)

Scripted producers (enqueued in this order against a FIFO server):

  A   priority  1    delay  0s    "Task A (Low Pri, Immediate)"
  B   priority  5    delay  0s    "Task B (Medium Pri, Immediate)"
  C   priority 10    delay  4s    "Task C (High Pri, 4s Delay)"
  D   priority  5    delay  0s    "Task D (Medium Pri, Immediate - Tie Check)"
  E   priority 10    delay  2s    "Task E (High Pri, 2s Delay)"

Why the expected order is B → D → A → E → C:

  B before D     same priority 5; FIFO picks B (enqueued first, smaller Seq)
  B,D before A   priority 5 beats priority 1
  A before E,C   E and C are still sitting in the delayed heap when A is the
                 only remaining visible message, so they cannot cut in yet
  E before C     E's 2s delay expires first; C stays invisible until +4s

`)
}

func availability(t task) string {
	if t.DelaySeconds == 0 {
		return "available immediately"
	}
	return fmt.Sprintf("hidden until +%ds", t.DelaySeconds)
}

func pollOnce(c *http.Client, base string, start time.Time, pending *[]*tracked, actual *[]*tracked) error {
	now := time.Now()
	elapsed := now.Sub(start)
	msg, err := dequeue(c, base)
	if err != nil {
		return err
	}
	ts := fmt.Sprintf("%s  t=%+6.2fs", now.Format("15:04:05.000"), elapsed.Seconds())

	if msg == nil {
		fmt.Printf("  %s  %s  %s\n", dim(ts), yellow("empty"), dim(explainEmpty(*pending, now)))
		return nil
	}

	got, rest, ok := takePending(*pending, msg)
	if !ok {
		got = &tracked{
			task: task{Label: "?", Payload: msg.Payload, Priority: msg.Priority},
			msg:  *msg,
		}
	}
	*pending = rest
	*actual = append(*actual, got)

	fmt.Printf("  %s  %s  %s\n", cyan(ts), green("DEQUEUED "+got.Label), got.Payload)
	fmt.Printf("      priority=%d  original delay=%ds  seq=%d\n", got.Priority, got.DelaySeconds, got.msg.Seq)
	fmt.Printf("      why: %s\n", explainDequeue(got, *pending, now))
	return nil
}

func takePending(pending []*tracked, msg *model.Message) (*tracked, []*tracked, bool) {
	for i, p := range pending {
		if p.msg.ID == msg.ID || p.Payload == msg.Payload {
			out := make([]*tracked, 0, len(pending)-1)
			out = append(out, pending[:i]...)
			out = append(out, pending[i+1:]...)
			p.msg = *msg
			return p, out, true
		}
	}
	return nil, pending, false
}

func explainDequeue(got *tracked, remaining []*tracked, now time.Time) string {
	var parts []string

	if got.DelaySeconds > 0 {
		since := now.Sub(got.msg.AvailableAt)
		if since < 800*time.Millisecond {
			parts = append(parts, fmt.Sprintf("%s just became visible after its %ds delay (promoted off the delayed heap)", got.Label, got.DelaySeconds))
		} else {
			parts = append(parts, fmt.Sprintf("%s has been visible since its %ds delay elapsed", got.Label, got.DelaySeconds))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%s was immediately visible (delay 0s)", got.Label))
	}

	var hidden, othersVisible []*tracked
	for _, r := range remaining {
		if now.Before(r.msg.AvailableAt) {
			hidden = append(hidden, r)
		} else {
			othersVisible = append(othersVisible, r)
		}
	}

	if len(hidden) > 0 {
		parts = append(parts, fmt.Sprintf("still delayed (invisible): %s", describeTracked(hidden, now)))
	}

	if len(othersVisible) == 0 {
		if len(hidden) > 0 {
			var higher []*tracked
			for _, h := range hidden {
				if h.Priority > got.Priority {
					higher = append(higher, h)
				}
			}
			if len(higher) > 0 {
				parts = append(parts, fmt.Sprintf("%s is the only visible candidate; higher-priority delayed messages (%s) cannot cut in until AvailableAt", got.Label, describePri(higher)))
			} else {
				parts = append(parts, fmt.Sprintf("%s is the only visible candidate; delayed messages stay invisible until AvailableAt", got.Label))
			}
		} else {
			parts = append(parts, fmt.Sprintf("%s is the last remaining message", got.Label))
		}
		return joinWhy(parts)
	}

	var samePri, lowerPri []*tracked
	for _, o := range othersVisible {
		switch {
		case o.Priority == got.Priority:
			samePri = append(samePri, o)
		case o.Priority < got.Priority:
			lowerPri = append(lowerPri, o)
		}
	}

	if len(samePri) > 0 {
		parts = append(parts, fmt.Sprintf("tied at priority %d with %s; FIFO selects the smallest Seq, so %s (seq %d) wins over %s",
			got.Priority, labels(samePri), got.Label, got.msg.Seq, labels(samePri)))
	} else {
		parts = append(parts, fmt.Sprintf("highest visible priority is %d, beating %s", got.Priority, describePri(lowerPri)))
	}
	return joinWhy(parts)
}

func explainEmpty(remaining []*tracked, now time.Time) string {
	if len(remaining) == 0 {
		return "queue empty — all scripted messages have been delivered"
	}
	var visible, hidden []*tracked
	for _, r := range remaining {
		if now.Before(r.msg.AvailableAt) {
			hidden = append(hidden, r)
		} else {
			visible = append(visible, r)
		}
	}
	if len(visible) > 0 {
		return "unexpected: visible messages remain (" + labels(visible) + ") but dequeue returned 204"
	}
	return "waiting on delay: " + describeTracked(hidden, now)
}

func labels(ts []*tracked) string {
	s := make([]string, len(ts))
	for i, t := range ts {
		s[i] = t.Label
	}
	return strings.Join(s, ", ")
}

func describePri(ts []*tracked) string {
	s := make([]string, len(ts))
	for i, t := range ts {
		s[i] = fmt.Sprintf("%s (pri %d)", t.Label, t.Priority)
	}
	return strings.Join(s, ", ")
}

func describeTracked(ts []*tracked, now time.Time) string {
	s := make([]string, len(ts))
	for i, t := range ts {
		wait := t.msg.AvailableAt.Sub(now)
		if wait < 0 {
			wait = 0
		}
		s[i] = fmt.Sprintf("%s (pri %d, ready in %.2fs)", t.Label, t.Priority, wait.Seconds())
	}
	return strings.Join(s, "; ")
}

func joinWhy(parts []string) string {
	return strings.Join(parts, ". ") + "."
}

func printSummary(expected []string, actual []*tracked) {
	fmt.Println(bold("── 3. Summary: expected vs actual dequeue order ───────────────────"))
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tExpected\tActual\tMatch")
	fmt.Fprintln(w, "  --\t--------\t------\t-----")

	n := len(expected)
	if len(actual) > n {
		n = len(actual)
	}
	allOK := true
	for i := 0; i < n; i++ {
		exp, act, match := "—", "—", "✗"
		if i < len(expected) {
			exp = expected[i] + "  " + payloadForLabel(expected[i])
		}
		if i < len(actual) {
			act = actual[i].Label + "  " + actual[i].Payload
		}
		if i < len(expected) && i < len(actual) && actual[i].Label == expected[i] {
			match = "✓"
		} else {
			allOK = false
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n", i+1, exp, act, match)
	}
	_ = w.Flush()
	fmt.Println()
	if allOK && len(actual) == len(expected) {
		fmt.Println(green("  Result: PASS  — Frankenstein scheduling matched the scripted order."))
	} else {
		fmt.Println(yellow("  Result: MISMATCH  — see the table. Confirm the server is in FIFO mode on an empty WAL."))
	}
	fmt.Println()
}

func orderMatches(expected []string, actual []*tracked) bool {
	if len(actual) != len(expected) {
		return false
	}
	for i := range expected {
		if actual[i].Label != expected[i] {
			return false
		}
	}
	return true
}

func payloadForLabel(label string) string {
	switch label {
	case "A":
		return "Task A (Low Pri, Immediate)"
	case "B":
		return "Task B (Medium Pri, Immediate)"
	case "C":
		return "Task C (High Pri, 4s Delay)"
	case "D":
		return "Task D (Medium Pri, Immediate - Tie Check)"
	case "E":
		return "Task E (High Pri, 2s Delay)"
	default:
		return ""
	}
}

func checkHealth(c *http.Client, base string) error {
	resp, err := c.Get(base + "/health")
	if err != nil {
		return fmt.Errorf("server not reachable at %s: %w", base, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET /health: unexpected status %d", resp.StatusCode)
	}
	var h healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&h); err != nil {
		return fmt.Errorf("GET /health: decode: %w", err)
	}
	if h.Status != "healthy" {
		return fmt.Errorf("GET /health: status %q, want healthy", h.Status)
	}
	return nil
}

func getStats(c *http.Client, base string) (statsResponse, error) {
	var st statsResponse
	resp, err := c.Get(base + "/stats")
	if err != nil {
		return st, fmt.Errorf("GET /stats: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return st, fmt.Errorf("GET /stats: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return st, fmt.Errorf("GET /stats: decode: %w", err)
	}
	return st, nil
}

func enqueue(c *http.Client, base string, req model.EnqueueRequest) (model.Message, error) {
	var msg model.Message
	body, err := json.Marshal(req)
	if err != nil {
		return msg, err
	}
	resp, err := c.Post(base+"/enqueue", "application/json", bytes.NewReader(body))
	if err != nil {
		return msg, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return msg, err
	}
	if resp.StatusCode != http.StatusCreated {
		return msg, fmt.Errorf("POST /enqueue: status %d: %s", resp.StatusCode, apiErr(raw))
	}
	if err := json.Unmarshal(raw, &msg); err != nil {
		return msg, fmt.Errorf("POST /enqueue: decode: %w", err)
	}
	return msg, nil
}

func dequeue(c *http.Client, base string) (*model.Message, error) {
	resp, err := c.Post(base+"/dequeue", "application/json", http.NoBody)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("POST /dequeue: status %d: %s", resp.StatusCode, apiErr(raw))
	}
	var msg model.Message
	if err := json.Unmarshal(raw, &msg); err != nil {
		return nil, fmt.Errorf("POST /dequeue: decode: %w", err)
	}
	return &msg, nil
}

func apiErr(raw []byte) string {
	var e errorResponse
	if json.Unmarshal(raw, &e) == nil && e.Error != "" {
		return e.Error
	}
	s := strings.TrimSpace(string(raw))
	if s == "" {
		return "(empty body)"
	}
	return s
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func bold(s string) string   { return "\033[1m" + s + "\033[0m" }
func dim(s string) string    { return "\033[2m" + s + "\033[0m" }
func green(s string) string  { return "\033[32m" + s + "\033[0m" }
func yellow(s string) string { return "\033[33m" + s + "\033[0m" }
func cyan(s string) string   { return "\033[36m" + s + "\033[0m" }
