// Command client is a small checkout-dispatch worker. It talks to a running
// Queuemaxxing server the way a real app would: enqueue jobs from a storefront
// burst, then dequeue and "process" them. The job mix is chosen so delay,
// priority, and FIFO/LIFO tie-breaking all show up in one run
// (ARCHITECTURE.md §4).
//
// The worker makes no assumptions about the server it is pointed at. It reads
// the ordering mode from GET /messages and predicts the drain order by
// replaying the engine's own three rules over the server's snapshot, so it is
// correct against a FIFO or a LIFO server and against a queue that already
// holds other traffic. A mismatch reported here is a real disagreement with
// the engine, never a mis-set expectation.
package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"queuemaxxing/pkg/model"
)

const (
	// pollInterval is how often the worker asks for its next job. It also
	// feeds the prediction: the simulation advances its clock by one
	// interval per pop, so a message maturing mid-drain is placed where
	// this worker will actually reach it.
	pollInterval = 500 * time.Millisecond

	// idleGrace is how long the worker keeps polling after its own jobs are
	// all processed, so a trailing "shop is caught up" line is printed.
	idleGrace = 1500 * time.Millisecond

	// snapshotLimit is the GET /messages page size used for prediction.
	snapshotLimit = 1000
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "client: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	url := flag.String("url", getenv("QUEUE_URL", "http://127.0.0.1:8080"), "base URL of the running Queuemaxxing server (env: QUEUE_URL)")
	colorMode := flag.String("color", getenv("QM_COLOR", "auto"), "colorize output: auto, always, or never (env: QM_COLOR)")
	flag.Parse()
	base := strings.TrimRight(strings.TrimSpace(*url), "/")

	if err := setColor(*colorMode); err != nil {
		return err
	}

	httpClient := &http.Client{Timeout: 5 * time.Second}

	if err := checkHealth(httpClient, base); err != nil {
		return fmt.Errorf("%w\nStart the server first:  go run ./cmd/server", err)
	}

	// The pre-flight snapshot tells us two things we must not guess: which
	// tie-break rule this server was started with, and whether other traffic
	// is already queued ahead of us.
	before, err := getSnapshot(httpClient, base)
	if err != nil {
		return err
	}

	printBanner(before.Mode)

	fmt.Printf("Connected to %s  (mode=%s)\n", base, before.Mode)
	if before.Stats.Total > 0 {
		fmt.Println(yellow(fmt.Sprintf(
			"  Note: the queue already holds %d message(s) (%d ready, %d delayed).\n"+
				"  That is fine - the worker scores only the jobs it enqueued, and the\n"+
				"  prediction below accounts for the existing traffic draining first.",
			before.Stats.Total, before.Stats.Available, before.Stats.Delayed)))
	}
	fmt.Println()

	script := []task{
		{Label: "A", Priority: 1, DelaySeconds: 0, Payload: "email receipt for order 1841"},
		{Label: "B", Priority: 5, DelaySeconds: 0, Payload: "capture payment for order 1842"},
		{Label: "C", Priority: 10, DelaySeconds: 8, Payload: "retry Stripe webhook for order 1842"},
		{Label: "D", Priority: 5, DelaySeconds: 0, Payload: "capture payment for order 1843"},
		{Label: "E", Priority: 10, DelaySeconds: 4, Payload: "retry inventory webhook for order 1843"},
	}

	fmt.Println(bold("-- 1. Storefront burst (enqueue) -----------------------------------"))
	mine := make(map[string]*tracked, len(script))
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
		mine[msg.ID] = tr
		pending = append(pending, tr)
		fmt.Printf("  %s  seq=%-4d pri=%-3d delay=%ds  %s\n",
			bold(t.Label), msg.Seq, t.Priority, t.DelaySeconds, availability(t))
	}

	// Predict from the server's own snapshot, using the engine's rules. This
	// is the expectation the summary is scored against.
	after, err := getSnapshot(httpClient, base)
	if err != nil {
		return err
	}
	expected := predictLabels(after, mine)
	fmt.Printf("\nWorker should process, in order: %s\n", bold(strings.Join(expected, " -> ")))
	fmt.Printf("%s\n\n", dim(reasoning(after.Mode, before.Stats.Total)))

	fmt.Println(bold(fmt.Sprintf("-- 2. Worker loop (POST /dequeue every %dms) ------------------------", pollInterval.Milliseconds())))
	actual, err := workerLoop(httpClient, base, mine, &pending, script)
	if err != nil {
		return err
	}

	fmt.Println()
	printSummary(expected, actual, script)
	if !orderMatches(expected, actual) {
		return fmt.Errorf("dequeue order did not match the engine's own rules (see table above)")
	}
	return nil
}

// workerLoop dequeues on a fixed tick until every job this run enqueued has
// been processed, then lingers briefly so the caught-up state is visible. It
// runs until a deadline derived from the longest delay in the script, so a
// slow backoff cannot be mistaken for a hang.
func workerLoop(c *http.Client, base string, mine map[string]*tracked, pending *[]*tracked, script []task) ([]*tracked, error) {
	var maxDelay int64
	for _, t := range script {
		if t.DelaySeconds > maxDelay {
			maxDelay = t.DelaySeconds
		}
	}
	deadline := time.Now().Add(time.Duration(maxDelay)*time.Second + 30*time.Second)

	start := time.Now()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var actual []*tracked
	var doneAt time.Time

	for {
		if err := pollOnce(c, base, start, mine, pending, &actual); err != nil {
			return actual, err
		}
		if len(actual) == len(mine) {
			if doneAt.IsZero() {
				doneAt = time.Now()
			}
			if time.Since(doneAt) >= idleGrace {
				return actual, nil
			}
		}
		if time.Now().After(deadline) {
			return actual, fmt.Errorf("timed out after %s with %d/%d jobs processed",
				time.Since(start).Round(time.Second), len(actual), len(mine))
		}
		<-ticker.C
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

type healthResponse struct {
	Status string `json:"status"`
}

type errorResponse struct {
	Error string `json:"error"`
}

func printBanner(mode model.QueueMode) {
	tie := "FIFO does the oldest job first"
	if mode == model.ModeLIFO {
		tie = "LIFO does the newest job first"
	}
	fmt.Printf(`
+----------------------------------------------------------------------+
|     Checkout dispatcher  (a worker that uses Queuemaxxing)            |
+----------------------------------------------------------------------+

You are the single worker behind a small shop checkout pipeline.
Five jobs just landed from the storefront. Process them in the order
the queue decides, not the order they arrived.

  A   pri  1   now     email receipt for order 1841
  B   pri  5   now     capture payment for order 1842
  C   pri 10   +8s     retry Stripe webhook for order 1842
  D   pri  5   now     capture payment for order 1843
  E   pri 10   +4s     retry inventory webhook for order 1843

This server is in %s mode, so on a priority tie: %s.

`, mode, tie)
}

// reasoning explains the predicted order in the vocabulary of the three
// rules, phrased for the mode the server actually runs.
//
// The walk-through below describes the five jobs draining on their own. When
// the queue already held other traffic, that traffic drains first and the
// backoffs mature while it does, which legitimately reorders our jobs — so
// the canned walk-through would be wrong and we explain the interleaving
// instead. The prediction itself is computed from the snapshot either way.
func reasoning(mode model.QueueMode, preexisting int) string {
	if preexisting > 0 {
		return "  The order above accounts for the " + fmt.Sprint(preexisting) + " message(s) already queued:\n" +
			"  they drain first, and our backoffs mature while they do, so a delayed\n" +
			"  priority-10 webhook can be ready before the captures are reached.\n" +
			"  Run against an empty queue to see the five jobs in isolation."
	}
	tie := "  B before D      two payment captures, same priority; FIFO takes the oldest"
	if mode == model.ModeLIFO {
		tie = "  D before B      two payment captures, same priority; LIFO takes the newest"
	}
	return tie + "\n" +
		"  captures first  money outranks the receipt email (priority 5 beats 1)\n" +
		"  A before E,C    the webhooks asked us to wait; they cannot jump the line yet\n" +
		"  E before C      the 4s inventory backoff ends before the 8s Stripe backoff"
}

func availability(t task) string {
	if t.DelaySeconds == 0 {
		return "available immediately"
	}
	return fmt.Sprintf("hidden until +%ds", t.DelaySeconds)
}

/* ---------------- prediction: the engine's rules, replayed ---------------- */

type simItem struct {
	id          string
	priority    int
	seq         uint64
	availableAt time.Time
}

// predictLabels returns the labels of this run's jobs in the order the engine
// will hand them over, derived from snap by replaying the same three rules the
// engine applies. Messages that are not ours interleave by the same rules and
// are dropped from the result, which leaves our jobs in their true relative
// order.
func predictLabels(snap model.QueueSnapshot, mine map[string]*tracked) []string {
	out := make([]string, 0, len(mine))
	for _, it := range simulateDrain(snap) {
		if tr, ok := mine[it.id]; ok {
			out = append(out, tr.Label)
		}
	}
	return out
}

// simulateDrain replays the drain of the whole queue. The clock advances by
// one poll interval per pop (this worker takes one job per tick) and jumps
// forward when only delayed messages remain, so delay maturation lands where
// the worker will actually observe it.
func simulateDrain(snap model.QueueSnapshot) []simItem {
	ready := make([]simItem, 0, len(snap.Ready))
	for _, m := range snap.Ready {
		ready = append(ready, simItem{m.ID, m.Priority, m.Seq, m.AvailableAt})
	}
	delayed := make([]simItem, 0, len(snap.Delayed))
	for _, m := range snap.Delayed {
		delayed = append(delayed, simItem{m.ID, m.Priority, m.Seq, m.AvailableAt})
	}
	sort.SliceStable(delayed, func(i, j int) bool {
		return delayed[i].availableAt.Before(delayed[j].availableAt)
	})

	less := lessFor(snap.Mode)
	t := snap.Now
	out := make([]simItem, 0, len(ready)+len(delayed))
	for len(ready) > 0 || len(delayed) > 0 {
		for len(delayed) > 0 && !delayed[0].availableAt.After(t) {
			ready = append(ready, delayed[0])
			delayed = delayed[1:]
		}
		if len(ready) == 0 {
			t = delayed[0].availableAt
			continue
		}
		sort.SliceStable(ready, func(i, j int) bool { return less(ready[i], ready[j]) })
		out = append(out, ready[0])
		ready = ready[1:]
		t = t.Add(pollInterval)
	}
	return out
}

// lessFor mirrors pkg/engine: highest priority first, ties by sequence number
// in the direction the mode dictates.
func lessFor(mode model.QueueMode) func(a, b simItem) bool {
	return func(a, b simItem) bool {
		if a.priority != b.priority {
			return a.priority > b.priority
		}
		if mode == model.ModeLIFO {
			return a.seq > b.seq
		}
		return a.seq < b.seq
	}
}

/* ---------------- the worker tick ---------------- */

func pollOnce(c *http.Client, base string, start time.Time, mine map[string]*tracked, pending *[]*tracked, actual *[]*tracked) error {
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

	// A message we did not enqueue is other traffic on a shared queue. It is
	// processed and reported, but it is not scored.
	tr, ok := mine[msg.ID]
	if !ok {
		fmt.Printf("  %s  %s  %s\n", dim(ts), dim("processed (other traffic)"), dim(msg.Payload))
		fmt.Printf("      %s\n", dim(fmt.Sprintf(
			"priority=%d  seq=%d  not part of this run - it was already queued and outranked our jobs",
			msg.Priority, msg.Seq)))
		return nil
	}

	tr.msg = *msg
	*pending = removeTracked(*pending, tr)
	*actual = append(*actual, tr)

	fmt.Printf("  %s  %s  %s\n", cyan(ts), green("processed "+tr.Label), tr.Payload)
	fmt.Printf("      priority=%d  original delay=%ds  seq=%d\n", tr.Priority, tr.DelaySeconds, tr.msg.Seq)
	fmt.Printf("      why: %s\n", explainDequeue(tr, *pending, now))
	return nil
}

func removeTracked(pending []*tracked, want *tracked) []*tracked {
	out := pending[:0]
	for _, p := range pending {
		if p != want {
			out = append(out, p)
		}
	}
	return out
}

func explainDequeue(got *tracked, remaining []*tracked, now time.Time) string {
	var parts []string

	if got.DelaySeconds > 0 {
		since := now.Sub(got.msg.AvailableAt)
		if since < 800*time.Millisecond {
			parts = append(parts, fmt.Sprintf("%s just became ready after its %ds backoff", got.Label, got.DelaySeconds))
		} else {
			parts = append(parts, fmt.Sprintf("%s has been ready since its %ds backoff elapsed", got.Label, got.DelaySeconds))
		}
	} else {
		parts = append(parts, fmt.Sprintf("%s was ready immediately (no backoff)", got.Label))
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
		parts = append(parts, fmt.Sprintf("still waiting on backoff: %s", describeTracked(hidden, now)))
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
				parts = append(parts, fmt.Sprintf("%s is the only ready job; higher-priority webhooks (%s) are still on backoff", got.Label, describePri(higher)))
			} else {
				parts = append(parts, fmt.Sprintf("%s is the only ready job; delayed jobs stay hidden until backoff ends", got.Label))
			}
		} else {
			parts = append(parts, fmt.Sprintf("%s is the last remaining job", got.Label))
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
		parts = append(parts, fmt.Sprintf("tied at priority %d with %s; the tie-break took %s (seq %d) over %s",
			got.Priority, labels(samePri), got.Label, got.msg.Seq, labels(samePri)))
	} else {
		parts = append(parts, fmt.Sprintf("highest visible priority is %d, beating %s", got.Priority, describePri(lowerPri)))
	}
	return joinWhy(parts)
}

func explainEmpty(remaining []*tracked, now time.Time) string {
	if len(remaining) == 0 {
		return "shop is caught up: every checkout job has been processed"
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
		return "unexpected: ready jobs remain (" + labels(visible) + ") but dequeue returned 204"
	}
	return "waiting on backoff: " + describeTracked(hidden, now)
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

func printSummary(expected []string, actual []*tracked, script []task) {
	fmt.Println(bold("-- 3. What the worker ran ------------------------------------------"))
	fmt.Println()

	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "  #\tPredicted\tActual\tMatch")
	fmt.Fprintln(w, "  --\t---------\t------\t-----")

	n := len(expected)
	if len(actual) > n {
		n = len(actual)
	}
	allOK := len(actual) == len(expected)
	for i := 0; i < n; i++ {
		exp, act, match := "-", "-", "x"
		if i < len(expected) {
			exp = expected[i] + "  " + payloadForLabel(script, expected[i])
		}
		if i < len(actual) {
			act = actual[i].Label + "  " + actual[i].Payload
		}
		if i < len(expected) && i < len(actual) && actual[i].Label == expected[i] {
			match = "ok"
		} else {
			allOK = false
		}
		fmt.Fprintf(w, "  %d\t%s\t%s\t%s\n", i+1, exp, act, match)
	}
	_ = w.Flush()
	fmt.Println()
	if allOK {
		fmt.Println(green("  Result: PASS  the engine drained exactly as its own rules predict."))
	} else {
		fmt.Println(yellow("  Result: MISMATCH  the engine disagreed with its own rules - see the table."))
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

func payloadForLabel(script []task, label string) string {
	for _, t := range script {
		if t.Label == label {
			return t.Payload
		}
	}
	return ""
}

/* ---------------- HTTP ---------------- */

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

func getSnapshot(c *http.Client, base string) (model.QueueSnapshot, error) {
	var snap model.QueueSnapshot
	resp, err := c.Get(fmt.Sprintf("%s/messages?limit=%d", base, snapshotLimit))
	if err != nil {
		return snap, fmt.Errorf("GET /messages: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return snap, fmt.Errorf("GET /messages: unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&snap); err != nil {
		return snap, fmt.Errorf("GET /messages: decode: %w", err)
	}
	if !snap.Mode.Valid() {
		return snap, fmt.Errorf("GET /messages: server reported unknown mode %q", snap.Mode)
	}
	return snap, nil
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

/* ---------------- output styling ----------------
   Colors are opt-out and only enabled when stdout is a terminal, so piping a
   run into a file or a CI log yields clean text rather than escape sequences.
   NO_COLOR is honored (https://no-color.org). */

var useColor bool

func setColor(mode string) error {
	switch strings.ToLower(strings.TrimSpace(mode)) {
	case "always":
		useColor = true
	case "never":
		useColor = false
	case "auto", "":
		useColor = autoColor()
	default:
		return fmt.Errorf("invalid -color %q: must be auto, always, or never", mode)
	}
	return nil
}

func autoColor() bool {
	if os.Getenv("NO_COLOR") != "" {
		return false
	}
	if os.Getenv("TERM") == "dumb" {
		return false
	}
	fi, err := os.Stdout.Stat()
	if err != nil {
		return false
	}
	return fi.Mode()&os.ModeCharDevice != 0
}

func paint(code, s string) string {
	if !useColor {
		return s
	}
	return "\033[" + code + "m" + s + "\033[0m"
}

func bold(s string) string   { return paint("1", s) }
func dim(s string) string    { return paint("2", s) }
func green(s string) string  { return paint("32", s) }
func yellow(s string) string { return paint("33", s) }
func cyan(s string) string   { return paint("36", s) }
