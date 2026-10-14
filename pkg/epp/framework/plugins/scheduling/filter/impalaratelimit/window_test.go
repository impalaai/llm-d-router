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

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var t0 = time.Unix(1_700_000_000, 0)

// full returns counters with the four required families present.
func full(requests, prompt, generated, hits float64) counters {
	return counters{Requests: requests, Prompt: prompt, Generated: generated, LocalHits: hits,
		HasRequests: true, HasPrompt: true, HasGenerated: true, HasLocalHits: true}
}

// fill adds one sample per second for `seconds` seconds at the given per-minute rates,
// starting from zero at t0, and returns the time of the last sample.
func fill(w *window, rpm, promptPM, genPM, hitsPM float64, seconds int) time.Time {
	var at time.Time
	for s := 0; s <= seconds; s++ {
		m := float64(s) / 60
		at = t0.Add(time.Duration(s) * time.Second)
		w.add(at, full(rpm*m, promptPM*m, genPM*m, hitsPM*m))
	}
	return at
}

func TestWindow_SteadyRates(t *testing.T) {
	w := newWindow(60 * time.Second)
	last := fill(w, 30, 10_000, 1_000, 8_000, 60)
	r := w.rates(last)
	require.True(t, r.RPMMeasured)
	require.True(t, r.TPMMeasured)
	assert.InDelta(t, 30, r.RPM, 1e-6)
	// uncached = 10000 - 8000 = 2000/min, + 1000 generated
	assert.InDelta(t, 3_000, r.TPM, 1e-6)
}

func TestWindow_ExternalHitsAreSubtracted(t *testing.T) {
	w := newWindow(60 * time.Second)
	for s := 0; s <= 60; s++ {
		m := float64(s) / 60
		c := full(0, 10_000*m, 0, 4_000*m)
		c.ExternalHits, c.HasExternalHits = 5_000*m, true
		w.add(t0.Add(time.Duration(s)*time.Second), c)
	}
	assert.InDelta(t, 1_000, w.rates(t0.Add(60*time.Second)).TPM, 1e-6)
}

func TestWindow_UncachedIsClampedAtZero(t *testing.T) {
	w := newWindow(60 * time.Second)
	// hits counted at scheduling can run ahead of prompt tokens counted at prefill completion
	last := fill(w, 0, 1_000, 500, 1_200, 60)
	assert.InDelta(t, 500, w.rates(last).TPM, 1e-6)
}

func TestWindow_WarmUp(t *testing.T) {
	w := newWindow(60 * time.Second)
	assert.Equal(t, rates{}, w.rates(t0), "no samples")
	w.add(t0, full(0, 0, 0, 0))
	assert.False(t, w.rates(t0).RPMMeasured, "one sample")
	w29 := newWindow(60 * time.Second)
	last := fill(w29, 30, 0, 0, 0, 29)
	assert.False(t, w29.rates(last).RPMMeasured, "29 s span < 30 s")
	w30 := newWindow(60 * time.Second)
	last = fill(w30, 30, 0, 0, 0, 30)
	assert.True(t, w30.rates(last).RPMMeasured, "30 s span = window/2")
}

func TestWindow_StaleSamplesAreUnmeasured(t *testing.T) {
	w := newWindow(60 * time.Second)
	last := fill(w, 30, 0, 0, 0, 60)
	assert.True(t, w.rates(last.Add(15*time.Second)).RPMMeasured, "newest sample 15 s old = window/4")
	assert.False(t, w.rates(last.Add(16*time.Second)).RPMMeasured, "older than window/4")
}

func TestWindow_TrimsToTheWindow(t *testing.T) {
	w := newWindow(60 * time.Second)
	fill(w, 30, 0, 0, 0, 300)
	assert.LessOrEqual(t, len(w.samples), 61)
	assert.LessOrEqual(t, w.samples[len(w.samples)-1].at.Sub(w.samples[0].at), 60*time.Second)
}

func TestWindow_RateChangeIsSeenWithinTheWindow(t *testing.T) {
	w := newWindow(60 * time.Second)
	last := fill(w, 30, 0, 0, 0, 60) // 30 rpm for a minute
	base := full(30, 0, 0, 0)
	for s := 1; s <= 60; s++ { // then 90 rpm for a minute
		at := last.Add(time.Duration(s) * time.Second)
		w.add(at, full(base.Requests+90*float64(s)/60, 0, 0, 0))
	}
	assert.InDelta(t, 90, w.rates(last.Add(60*time.Second)).RPM, 1e-6)
}

func TestWindow_CounterDecreaseRestartsTheWindow(t *testing.T) {
	w := newWindow(60 * time.Second)
	last := fill(w, 30, 1_000, 100, 0, 60)
	restart := last.Add(time.Second)
	w.add(restart, full(1, 5, 1, 0)) // engine restarted: counters near zero
	require.Len(t, w.samples, 1)
	r := w.rates(restart)
	assert.False(t, r.RPMMeasured)
	assert.False(t, r.TPMMeasured)
}

func TestWindow_FamilyAppearingOrDisappearingRestartsTheWindow(t *testing.T) {
	w := newWindow(60 * time.Second)
	last := fill(w, 30, 1_000, 100, 0, 60)
	c := full(31, 1_001, 101, 0)
	c.ExternalHits, c.HasExternalHits = 1, true
	w.add(last.Add(time.Second), c)
	assert.Len(t, w.samples, 1)
}

func TestWindow_MissingFamiliesLeaveTheirDimensionUnmeasured(t *testing.T) {
	w := newWindow(60 * time.Second)
	for s := 0; s <= 60; s++ {
		m := float64(s) / 60
		w.add(t0.Add(time.Duration(s)*time.Second), counters{Requests: 30 * m, HasRequests: true,
			Prompt: 1_000 * m, HasPrompt: true, Generated: 100 * m, HasGenerated: true}) // no local hits
	}
	r := w.rates(t0.Add(60 * time.Second))
	assert.True(t, r.RPMMeasured)
	assert.False(t, r.TPMMeasured, "TPM never silently becomes total tokens")
}

func TestWindow_Due(t *testing.T) {
	w := newWindow(60 * time.Second)
	assert.True(t, w.due(t0))
	w.add(t0, full(0, 0, 0, 0))
	assert.False(t, w.due(t0.Add(999*time.Millisecond)))
	assert.True(t, w.due(t0.Add(time.Second)))
}
