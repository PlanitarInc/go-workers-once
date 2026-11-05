## Project
go-workers-once — a Go library (package `once`) providing middleware for go-workers that prevents jobs of the same type from running in parallel, plus helpers to enqueue "once" jobs and wait for them to finish.

## Stack
- Go 1.17 (`go.mod`)
- Redis via `github.com/gomodule/redigo`
- `github.com/PlanitarInc/go-workers` — Planitar's fork, not `jrallison/go-workers`; the fork adds `PrepareEnqueuMsg()`/`EnqueueMsg()`, which this library needs
- Tests: standard `testing` with Gomega

## Structure
- `enqueue.go` — `Enqueue`, `EnqueueIn`, `EnqueueForce`, `EnqueueForceIn`: write a job descriptor to Redis, then enqueue the job
- `middleware.go` — `Middleware`, appended to `workers.Middleware`; moves the descriptor through its statuses as the job runs, fails or is retried
- `wait.go` — `WaitForJobType` / `GetDesc`: read a descriptor, or block until the job is done (Redis pub/sub)
- `job-descriptor.go` — `JobDesc`, `Options` (timeouts/retention) and the status constants
- `update_status.lua` — atomic, JID-checked status update; embedded with `//go:embed` in `scripts.go`
- `setup_test.go` — test Redis config (`REDIS_HOST`, `REDIS_PORT`)

The descriptor is a JSON value at the Redis key `<namespace>once:q:<queue>:<jobType>`, and a copy travels on the job message as `x-once`. The enqueue side, the middleware and the waiters all build that key on their own, so change all three together.

## Commands
- Build: `go build ./...`
- Test: `go test ./...` (needs Redis on `localhost:6379`; override with `REDIS_HOST` / `REDIS_PORT`, e.g. `docker run --rm -p 16379:6379 redis:alpine` then `REDIS_PORT=16379 go test ./...`)
- Single test: `go test -run TestNewJobDesc .`
- Lint CLAUDE.md: `make lint-claude-md`

## Verification
After every change, run in this order:
1. `go build ./...` — fix compile errors
2. `go test ./...` — fix failing tests (CI runs `go test -v ./...` against Redis)
3. `make lint-claude-md` — keep CLAUDE.md under the word cap

## Conventions
- Tests call `cleanRedis()`, which runs `FLUSHDB` on Redis database 15 of whatever Redis they connect to. Point them only at a disposable Redis; on machines running the wi-api dev stack, 6379 is that stack's Redis.
- Tests are plain `func TestX(t *testing.T)` with `RegisterTestingT(t)` and Gomega matchers; no Ginkgo.
- Status transitions on an existing descriptor go through `update_status.lua` (via `updateJobStatus*`); only creation (enqueue) and `reclaimJobDesc` use a direct `SET ... NX`/`XX`. The script returns `-1` for a missing key and `-2` for a JID mismatch, and the middleware depends on those codes.
- The Lua script is embedded at compile time; there is no generate step, so `make` / `make build` (`go generate ./...`) does nothing. GitHub Actions is the CI; `.travis.yml`, the README's `go generate` / `lua/script.go` release steps and its `EnqueueOnce` example are out of date.
- CLAUDE.md is the checked-in agent spec ([LADR-0016](https://docs.plntr.dev/ladrs/0016-checked-in-claude-md-per-repo)).
