/*
Copyright 2026 The llm-d Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package impalaratelimit

import "time"

// sampleEvery is the window's resolution: at most one stored sample per endpoint per second,
// however often the EPP polls (every 50 ms by default).
const sampleEvery = time.Second

type sample struct {
	at time.Time
	c  counters
}

// rates is one endpoint's measured throughput. A dimension is usable only when measured.
type rates struct {
	RPM, TPM                 float64
	RPMMeasured, TPMMeasured bool
}

// window keeps the samples of the last `length`.
type window struct {
	length  time.Duration
	samples []sample
}

func newWindow(length time.Duration) *window { return &window{length: length} }

// due reports whether a poll at now should be stored, so most polls cost no parsing.
func (w *window) due(now time.Time) bool {
	n := len(w.samples)
	return n == 0 || now.Sub(w.samples[n-1].at) >= sampleEvery
}

// add stores a sample and trims the window. A counter that went DOWN, or a family that
// appeared or disappeared, means the engine restarted or changed image: the deltas across
// that point are meaningless, so the window restarts from this sample.
func (w *window) add(now time.Time, c counters) {
	if n := len(w.samples); n > 0 && discontinuous(w.samples[n-1].c, c) {
		w.samples = w.samples[:0]
	}
	w.samples = append(w.samples, sample{at: now, c: c})
	cut := 0
	for cut < len(w.samples)-1 && now.Sub(w.samples[cut].at) > w.length {
		cut++
	}
	w.samples = w.samples[cut:]
}

func discontinuous(prev, cur counters) bool {
	if prev.HasRequests != cur.HasRequests || prev.HasPrompt != cur.HasPrompt ||
		prev.HasGenerated != cur.HasGenerated || prev.HasLocalHits != cur.HasLocalHits ||
		prev.HasExternalHits != cur.HasExternalHits {
		return true
	}
	return cur.Requests < prev.Requests || cur.Prompt < prev.Prompt || cur.Generated < prev.Generated ||
		cur.LocalHits < prev.LocalHits || cur.ExternalHits < prev.ExternalHits
}

// rates computes per-minute rates from the oldest and newest kept samples. A dimension is
// measured only once the samples span half the window and the newest is no older than a
// quarter of it (a fresh EPP, or an endpoint whose polls stopped, is never limited on a
// guess). TPM is uncached prompt tokens (clamped at 0: hits are counted at scheduling, prompt
// tokens at prefill completion) plus generated tokens; it needs the local-hits family, so a
// version without it is unmeasured rather than silently counted as total tokens.
func (w *window) rates(now time.Time) rates {
	var r rates
	n := len(w.samples)
	if n < 2 {
		return r
	}
	first, last := w.samples[0], w.samples[n-1]
	span := last.at.Sub(first.at)
	if span < w.length/2 || now.Sub(last.at) > w.length/4 {
		return r
	}
	perMinute := 60 / span.Seconds()
	if last.c.HasRequests {
		r.RPM, r.RPMMeasured = (last.c.Requests-first.c.Requests)*perMinute, true
	}
	if last.c.HasPrompt && last.c.HasGenerated && last.c.HasLocalHits {
		uncached := (last.c.Prompt - first.c.Prompt) - (last.c.LocalHits - first.c.LocalHits) -
			(last.c.ExternalHits - first.c.ExternalHits)
		if uncached < 0 {
			uncached = 0
		}
		r.TPM, r.TPMMeasured = (uncached+(last.c.Generated-first.c.Generated))*perMinute, true
	}
	return r
}
