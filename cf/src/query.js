const originCache = new WeakMap()
    , originStackCache = new WeakMap()
    , originError = Symbol('OriginError')

export const CLOSE = {}
export class Query extends Promise {
  constructor(strings, args, handler, canceller, options = {}) {
    let resolve
      , reject

    super((a, b) => {
      resolve = a
      reject = b
    })

    this.tagged = Array.isArray(strings.raw)
    this.strings = strings
    this.args = args
    this.handler = handler
    this.canceller = canceller
    this.options = options

    this.state = null
    this.statement = null

    this.resolve = x => (this.active = false, this.settled = true, resolve(x))
    this.reject = x => (this.active = false, this.settled = true, reject(x))

    this.active = false
    this.settled = false
    this.cancelled = null
    this.executed = false
    this.startedAt = 0
    this.signature = ''

    this[originError] = this.handler.debug
      ? new Error()
      : this.tagged && cachedError(this.strings)
  }

  get origin() {
    return (this.handler.debug
      ? this[originError].stack
      : this.tagged && originStackCache.has(this.strings)
        ? originStackCache.get(this.strings)
        : originStackCache.set(this.strings, this[originError].stack).get(this.strings)
    ) || ''
  }

  static get [Symbol.species]() {
    return Promise
  }

  cancel() {
    if (!this.canceller)
      return

    // Hold the CancelRequest's promise and hand it back: it rejects when the
    // second connection it needs cannot be opened, and a dropped rejection
    // is an unhandledRejection — fatal in Node by default.
    const cancelling = this.canceller(this)
    this.canceller = null
    return cancelling
  }

  // Keep this query off a busy connection's pipeline, so that cancel()
  // dequeues it for as long as it has not started (see handler in index.js).
  cancellable() {
    this.options.cancellable = true
    return this
  }

  simple() {
    this.options.simple = true
    this.options.prepare = false
    return this
  }

  async readable() {
    this.simple()
    this.streaming = true
    return this
  }

  async writable() {
    this.simple()
    this.streaming = true
    return this
  }

  cursor(rows = 1, fn) {
    this.options.simple = false
    if (typeof rows === 'function') {
      fn = rows
      rows = 1
    }

    this.cursorRows = rows

    if (typeof fn === 'function')
      return (this.cursorFn = fn, this)

    let prev
      , error = null
    return {
      [Symbol.asyncIterator]: () => ({
        next: () => {
          // An error that lands between two next() calls - the connection
          // dying while the caller works on a batch - finds the promise it
          // would reject already resolved with that batch. Keep it for the
          // calls that follow, or the iteration ends as if every row was read.
          if (error)
            return Promise.reject(error)

          if (this.executed && !this.active)
            return { done: true }

          prev && prev()
          const promise = new Promise((resolve, reject) => {
            this.cursorFn = value => {
              resolve({ value, done: false })
              return new Promise(r => prev = r)
            }
            this.resolve = () => (this.active = false, resolve({ done: true }))
            this.reject = x => (this.active = false, error || (error = x), reject(x))
          })
          this.execute()
          return promise
        },
        return() {
          prev && prev(CLOSE)
          return { done: true }
        }
      })
    }
  }

  describe() {
    this.options.simple = false
    this.onlyDescribe = this.options.prepare = true
    return this
  }

  stream() {
    throw new Error('.stream has been renamed to .forEach')
  }

  forEach(fn) {
    this.forEachFn = fn
    this.handle()
    return this
  }

  raw() {
    this.isRaw = true
    return this
  }

  values() {
    this.isRaw = 'values'
    return this
  }

  async handle() {
    // A query cancelled before it was ever dispatched is already rejected, so
    // don't hand it to the pool: nothing would be written, and the connection
    // opened (or handed) to run it is left with no ReadyForQuery to release it.
    !this.executed && (this.executed = true) && await 1 && !this.cancelled && this.handler(this)
  }

  execute() {
    this.handle()
    return this
  }

  then() {
    this.handle()
    return super.then.apply(this, arguments)
  }

  catch() {
    this.handle()
    return super.catch.apply(this, arguments)
  }

  finally() {
    this.handle()
    return super.finally.apply(this, arguments)
  }
}

function cachedError(xs) {
  if (originCache.has(xs))
    return originCache.get(xs)

  const x = Error.stackTraceLimit
  Error.stackTraceLimit = 4
  const error = new Error()
  Error.stackTraceLimit = x
  // Format the stack now: an unformatted one holds its frames' functions
  // and receivers, and through their closures the first call's arguments,
  // for as long as the strings array lives.
  error.stack // eslint-disable-line
  originCache.set(xs, error)
  return error
}
