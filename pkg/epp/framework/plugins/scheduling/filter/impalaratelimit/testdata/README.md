# vLLM /metrics fixtures

Captured 2026-10-13 from live vLLM engines through the EPP's own scrape path, filtered to the five families
impala-rate-limit-filter reads. One file per live engine image reachable on 2026-10-13 (two further images could
not be captured: their endpoints returned HTTP 502). `counters_test.go` requires every file to yield the four
required families.

| File | Model | Families exported (`# TYPE` lines kept) |
|---|---|---|
| glm-5.3.txt | GLM-5.3 | vllm:prefix_cache_hits_total, vllm:prefix_cache_hits_created, vllm:external_prefix_cache_hits_total, vllm:external_prefix_cache_hits_created, vllm:prompt_tokens_total, vllm:prompt_tokens_created, vllm:prompt_tokens_by_source_total, vllm:prompt_tokens_by_source_created, vllm:prompt_tokens_cached_total, vllm:prompt_tokens_cached_created, vllm:generation_tokens_total, vllm:generation_tokens_created, vllm:time_to_first_token_seconds, vllm:time_to_first_token_seconds_created |
| glm-5.2-nvfp4.txt | zai-org/GLM-5.2-NVFP4 | same as glm-5.3.txt |
| deepseek-v4-flash.txt | DeepSeek-V4-Flash | same as glm-5.3.txt |
| gpt-oss-120b.txt | gpt-oss-120b | vllm:time_to_first_token_seconds, vllm:prefix_cache_hits_total, vllm:external_prefix_cache_hits_total, vllm:prompt_tokens_total, vllm:prompt_tokens_by_source_total, vllm:prompt_tokens_cached_total, vllm:generation_tokens_total (no `_created` families) |

Notes:

- Every counter `# TYPE` line in every file carries `_total`; no fixture needs the stripped-suffix fallback.
- Every captured engine declares `vllm:external_prefix_cache_hits_total` (the value is 0 where no KV connector is
  active, e.g. gpt-oss-120b), so the optional family is present in all four files.
- Two exposition shapes exist: the first three files also expose `*_created` gauge families (HELP/TYPE lines only,
  their samples are filtered out); gpt-oss-120b does not. A second GLM-5.3 engine from a different deployment
  exposed byte-identical HELP/TYPE lines, so it is not kept.
- The filter keeps the family-prefix `# HELP/# TYPE` lines, so `prompt_tokens_by_source_total` and
  `prompt_tokens_cached_total` appear as HELP/TYPE lines without samples. They are not read by the plugin.
