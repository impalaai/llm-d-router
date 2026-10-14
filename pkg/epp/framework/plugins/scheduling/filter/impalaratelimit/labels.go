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
	"errors"
	"fmt"
	"strconv"
)

// Endpoint labels the chart and the llmd-manager write into inventory.list (spec §5.2);
// file-discovery copies them verbatim into EndpointMetadata.Labels.
const (
	LabelLimitRPM = "impala.ai/limit-rpm"
	LabelLimitTPM = "impala.ai/limit-tpm"

	// maxLimit is the chart's and the manager's bound (nine digits).
	maxLimit = 999_999_999
)

// limits is one endpoint's ceilings; 0 means no limit on that dimension.
type limits struct {
	RPM int64
	TPM int64
}

func (l limits) none() bool { return l.RPM == 0 && l.TPM == 0 }

// parseLimits reads the two limit labels independently. A malformed value disables THAT
// dimension only (fail open) and is named in the returned error; the other still applies.
func parseLimits(labels map[string]string) (limits, error) {
	var l limits
	var errs []error
	for _, d := range []struct {
		key string
		dst *int64
	}{{LabelLimitRPM, &l.RPM}, {LabelLimitTPM, &l.TPM}} {
		raw, ok := labels[d.key]
		if !ok {
			continue
		}
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 1 || v > maxLimit {
			errs = append(errs, fmt.Errorf("%s=%q is not an integer in [1, %d]", d.key, raw, maxLimit))
			continue
		}
		*d.dst = v
	}
	return l, errors.Join(errs...)
}
