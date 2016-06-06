[![Build Status](https://travis-ci.org/PlanitarInc/go-workers-once.png)](https://travis-ci.org/PlanitarInc/go-workers-once)
[![GoDoc](https://godoc.org/github.com/PlanitarInc/go-workers-once?status.png)](https://godoc.org/github.com/PlanitarInc/go-workers-once)

A middleware for [go-workers](https://github.com/PlanitarInc/go-workers)
trying to avoid running same job type in parallel.

**IMPORTANT**: the middleware does not work on top of vanilla
[jrallison/go-workers](https://github.com/jrallison/go-workers),
but requires Planitar's fork
[PlanitarInc/go-workers](https://github.com/PlanitarInc/go-workers).
The fork adds a support for 2 additional functions:
`PrepareEnqueuMsg()` and `EnqueueMsg()`.

### Usage

Enqueue your tasks with `EnqueueOnce()`, add a customized middleware
to take care of these tasks.

### Example

```go
package main

import (
    "github.com/PlanitarInc/go-workers"
    "github.com/PlanitarInc/go-workers-once"
)

func main() {
  workers.Configure(map[string]string{
    // location of redis instance
    "server":  "localhost:6379",
    // instance of the database
    "database":  "0",
    // number of connections to keep open with redis
    "pool":    "30",
    // unique process id for this instance of workers (for proper recovery of inprogress jobs on crash)
    "process": "1",
  })

  workers.Middleware.Append(&once.Middleware{})
  workers.Process("myqueue", myJob, 20)

  // Enqueue jobs through once
  // The job of type `add-1-2` is added to the queue
  once.EnqueueOnce("myqueue", "add-1-2", []int{1, 2}, nil)
  // The job of type `add-1-2` is ignored
  once.EnqueueOnce("myqueue", "add-1-2", []int{1, 2}, nil)

  // Blocks until the enqueued job is done (either fails or succeeds)
  once.WaitForJobType("myqueue", "add-1-2")

  // Returns immediately since there are no jobs of type `add-1-2`
  // in the queue.
  once.WaitForJobType("myqueue", "add-1-2", WaitOptions{
    StopIfEmpty: true,
  })

  // Blocks until a job of type `add-1-2` is added to the queue.
  // In this example it will hang forever.
  once.WaitForJobType("myqueue", "add-1-2")
}
```
