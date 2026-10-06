import { setImmediate, clearImmediate } from '../polyfills.js'
// The timers and clocks the driver schedules with, taken once when it loads.
// A test runner's fake timers (vi.useFakeTimers(), Jest's, @sinonjs/fake-timers,
// Deno's FakeTime) replace the globals after the driver is imported; through
// these copies every query, connect and stream keeps real time while the test's
// own code sees the fake clock. Before, faking setImmediate alone hung every
// query (the write batching), and faking setTimeout every new connection.

const host = globalThis
    , realSetTimeout = host.setTimeout
    , realClearTimeout = host.clearTimeout
    , realSetInterval = host.setInterval
    , realClearInterval = host.clearInterval
    , realImmediate = typeof setImmediate === 'function' ? setImmediate : null
    , realClearImmediate = typeof clearImmediate === 'function' ? clearImmediate : null
    , realDateNow = Date.now
    , realPerformance = host.performance
    , realPerformanceNow = realPerformance.now

function timeout(fn, ms, ...args) {
  return realSetTimeout.call(host, fn, ms, ...args)
}

function untimeout(timer) {
  return realClearTimeout.call(host, timer)
}

function interval(fn, ms, ...args) {
  return realSetInterval.call(host, fn, ms, ...args)
}

function uninterval(timer) {
  return realClearInterval.call(host, timer)
}

function immediate(fn, ...args) {
  return realImmediate ? realImmediate(fn, ...args) : timeout(fn, 0, ...args)
}

function unimmediate(timer) {
  return realClearImmediate ? realClearImmediate(timer) : untimeout(timer)
}

// Milliseconds since the epoch, for the wire (a status update's timestamp).
function wallNow() {
  return realDateNow()
}

// Monotonic milliseconds, for every duration: a host clock step (NTP, a VM
// resume) never moves it.
function now() {
  return realPerformanceNow.call(realPerformance)
}

export {
  timeout as setTimeout,
  untimeout as clearTimeout,
  interval as setInterval,
  uninterval as clearInterval,
  immediate as setImmediate,
  unimmediate as clearImmediate,
  wallNow,
  now
}
