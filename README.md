[![Build Status](https://travis-ci.org/PlanitarInc/go-workers-once.png)](https://travis-ci.org/PlanitarInc/go-workers-once)
[![GoDoc](https://godoc.org/github.com/PlanitarInc/go-workers-once?status.png)](https://godoc.org/github.com/PlanitarInc/go-workers-once)

A middleware for [go-workers](https://github.com/PlanitarInc/go-workers)
trying to avoid running same job type in parallel.

**IMPORTANT**: the middleware does not work on top of vanilla
[jrallison/go-workers](https://github.com/jrallison/go-workers),
but requires a Planitar's fork
[PlanitarInc/go-workers](https://github.com/PlanitarInc/go-workers).
The fork adds a support for 2 additional functions:
`PrepareEnqueuMsg()` and `EnqueueMsg()`.
