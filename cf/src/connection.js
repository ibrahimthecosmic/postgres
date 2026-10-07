import { Buffer } from 'node:buffer'
import { net } from '../polyfills.js'
import { tls } from '../polyfills.js'
import { crypto } from '../polyfills.js'
import Stream from 'node:stream'

import { stringify, handleValue, arrayParser, arraySerializer } from './types.js'
import { Errors } from './errors.js'
import Result from './result.js'
import Queue from './queue.js'
import { Query, CLOSE } from './query.js'
import b from './bytes.js'
import { setTimeout, clearTimeout, setImmediate, clearImmediate, now } from './timers.js'

export default Connection

let uid = 1

const Sync = b().S().end()
    , Flush = b().H().end()
    , SSLRequest = b().i32(8).i32(80877103).end(8)
    , ExecuteUnnamed = Buffer.concat([b().E().str(b.N).i32(0).end(), Sync])
    , DescribeUnnamed = b().D().str('S').str(b.N).end()
    , noop = () => { /* noop */ }

// Parameter types the server described for a statement text, per pool (the
// options object is the pool's), so that a statement that is not prepared -
// prepare: false, or past what the caller prepares - is described once, not
// on every run. Bounded: the oldest description goes first.
const descriptions = new WeakMap()
    , maxDescriptions = 1024

function described(options) {
  let x = descriptions.get(options)
  x || descriptions.set(options, x = new Map())
  return x
}

function learn(options, key, types) {
  const x = described(options)
  x.delete(key)
  x.set(key, types.slice())
  x.size > maxDescriptions && x.delete(x.keys().next().value)
}

const retryRoutines = new Set([
  'FetchPreparedStatement',
  'RevalidateCachedQuery',
  'transformAssignedExpr'
])

const errorFields = {
  83  : 'severity_local',    // S
  86  : 'severity',          // V
  67  : 'code',              // C
  77  : 'message',           // M
  68  : 'detail',            // D
  72  : 'hint',              // H
  80  : 'position',          // P
  112 : 'internal_position', // p
  113 : 'internal_query',    // q
  87  : 'where',             // W
  115 : 'schema_name',       // s
  116 : 'table_name',        // t
  99  : 'column_name',       // c
  100 : 'data type_name',    // d
  110 : 'constraint_name',   // n
  70  : 'file',              // F
  76  : 'line',              // L
  82  : 'routine'            // R
}

function Connection(options, queues = {}, { onopen = noop, onend = noop, onclose = noop, onending = () => false, onresend = null } = {}) {
  const {
    sslnegotiation,
    ssl,
    max,
    user,
    host,
    port,
    database,
    parsers,
    transform,
    onnotice,
    onnotify,
    onparameter,
    max_pipeline,
    keep_alive,
    backoff,
    target_session_attrs
  } = options

  const sent = Queue()
      , id = uid++
      , backend = { pid: null, secret: null }
      , idleTimer = timer(end, options.idle_timeout)
      , lifeTimer = timer(end, options.max_lifetime)
      , connectTimer = timer(connectTimedOut, options.connect_timeout)

  let socket = null
    , refed = true
    , cancelMessage
    , errorResponse = null
    , result = new Result()
    , incoming = Buffer.alloc(0)
    , needsTypes = options.fetch_types
    , backendParameters = {}
    , statements = {}
    , statementId = Math.random().toString(36).slice(2)
    , statementCount = 1
    , closedTime = 0
    , remaining = 0
    , hostIndex = 0
    , retries = 0
    , length = 0
    , delay = 0
    , rows = 0
    , serverSignature = null
    , nextWriteTimer = null
    , terminated = false
    , incomings = null
    , results = null
    , initial = null
    , ending = null
    , stream = null
    , chunk = null
    , ended = null
    , nonce = null
    , query = null
    , idleFatal = null
    , lostError = null
    , final = null
    , again = []

  const connection = {
    queue: queues.closed,
    idleTimer,
    connect(query) {
      initial = query
      reconnect()
    },
    terminate,
    execute,
    cancel,
    lose,
    unanswered,
    end,
    ref,
    release,
    count: 0,
    id
  }

  queues.closed && queues.closed.push(connection)

  return connection

  async function createSocket() {
    let x
    try {
      x = options.socket
        ? (await Promise.resolve(options.socket(options)))
        : new net.Socket()
    } catch (e) {
      error(e)
      return
    }
    x.on('error', socketError)
    x.on('close', closed)
    x.on('drain', drain)
    return x
  }

  // Whether this connection's socket holds the process open (see move() in
  // index.js). A socket opened later starts ref'd, like the connection that
  // opens it for a query.
  function ref(x) {
    if (refed === x)
      return
    refed = x
    socket && (x ? socket.ref && socket.ref() : socket.unref && socket.unref())
  }

  // A reserve() handed this connection back. It rejoins the pool - or, when
  // end() was called while it was reserved, it closes: that end() waits on
  // it, and nothing else would ever settle it. With a query still in
  // flight, its ReadyForQuery does the same.
  function release() {
    connection.reserved = null
    if (!ending)
      return onopen(connection)
    query || sent.length || onending(connection) || terminate()
  }

  async function cancel({ pid, secret }, resolve, reject) {
    try {
      cancelMessage = b().i32(16).i32(80877102).i32(pid).i32(secret).end(16)
      await connect()
      socket.once('error', reject)
      socket.once('close', resolve)
    } catch (error) {
      reject(error)
    }
  }

  // Destroy the socket outright: unlike terminate(), which ends it politely
  // and waits for the server's side of the close, this works on a connection
  // whose far end stopped answering (half-open) - the close it causes fails
  // what the connection held with `error`, and the pool replaces it.
  function lose(error) {
    if (!socket)
      return
    lostError = error
    socket.destroy()
  }

  // A CancelRequest went out for q, running on this connection. A server that
  // is there answers it within a round trip or two (57014, or the statement's
  // own result); q still unsettled cancel_timeout seconds later is on a
  // connection that stopped answering, and every statement routed to it would
  // hang with it. Lose it instead.
  function unanswered(q) {
    const seconds = options.cancel_timeout
    if (!seconds)
      return
    const timer = setTimeout(() => {
      q.settled || lose(Errors.connection('CONNECTION_CLOSED', options, socket || undefined))
    }, seconds * 1000)
    timer.unref && timer.unref()
  }

  function execute(q) {
    if (terminated)
      return queryError(q, Errors.connection('CONNECTION_DESTROYED', options))

    // Only a caller holding on to a connection that has since closed gets
    // here; queued, the query would be answered by whatever reopens it.
    if (!socket)
      return queryError(q, idleFatal || Errors.connection('CONNECTION_CLOSED', options))

    if (stream)
      return queryError(q, Errors.generic('COPY_IN_PROGRESS', 'You cannot execute queries during copy'))

    if (q.cancelled)
      return

    try {
      q.state = backend
      query
        ? sent.push(q)
        : (query = q, start(query))

      build(q)
      return write(toBuffer(q))
        && !q.describeFirst
        && !q.cursorFn
        && sent.length < max_pipeline
        && (!q.options.onexecute || q.options.onexecute(connection))
    } catch (error) {
      sent.length === 0 && write(Sync)
      errored(error)
      return true
    }
  }

  function toBuffer(q) {
    if (q.parameters.length >= 65534)
      throw Errors.generic('MAX_PARAMETERS_EXCEEDED', 'Max number of parameters (65534) exceeded')

    return q.options.simple
      ? b().Q().str(q.statement.string + b.N).end()
      : q.describeFirst
        ? Buffer.concat([describe(q), Flush])
        : q.prepare
          ? q.prepared
            ? prepared(q)
            : Buffer.concat([describe(q), prepared(q)])
          : unnamed(q)
  }

  function describe(q) {
    return Buffer.concat([
      Parse(q.statement.string, q.parameters, q.statement.types, q.statement.name),
      Describe('S', q.statement.name)
    ])
  }

  function prepared(q) {
    return Buffer.concat([
      Bind(q.parameters, q.statement.types, q.statement.name, q.cursorName),
      q.cursorFn
        ? Execute('', q.cursorRows)
        : ExecuteUnnamed
    ])
  }

  function unnamed(q) {
    return Buffer.concat([
      Parse(q.statement.string, q.parameters, q.statement.types),
      DescribeUnnamed,
      prepared(q)
    ])
  }

  function build(q) {
    const parameters = []
        , types = []

    const string = stringify(q, q.strings[0], q.args[0], parameters, types, options)

    !q.tagged && q.args.forEach(x => handleValue(x, parameters, types, options))

    q.prepare = options.prepare && ('prepare' in q.options ? q.options.prepare : true)
    q.string = string
    q.signature = q.prepare && types + string
    q.onlyDescribe && (delete statements[q.signature])
    q.parameters = q.parameters || parameters
    q.prepared = q.prepare && q.signature in statements
    q.described = null
    // Describing first costs a round trip: Parse/Describe, wait for the
    // parameter types, then Bind/Execute. It is only needed for the types the
    // values did not pin, and only to pick their serializers - so not at all
    // when every type is pinned, nor when an earlier description of the same
    // text (any connection of this pool) already told us. Those statements go
    // out as one pipeline.
    if (parameters.length && !q.prepared && !q.onlyDescribe) {
      const known = types.every(x => x)
      q.described = known ? null : types + string
      const learned = !known && described(options).get(q.described)
      learned && learned.forEach((x, i) => types[i] = x)
      q.guessed = !!learned
      q.describeFirst = !known && !learned
    } else {
      q.guessed = false
      q.describeFirst = q.onlyDescribe || (parameters.length && !q.prepared)
    }
    q.statement = q.prepared
      ? statements[q.signature]
      : { string, types, name: q.prepare ? statementId + statementCount++ : '' }

    typeof options.debug === 'function' && options.debug(id, string, parameters, types)
  }

  function write(x, fn) {
    // closed() has failed everything that was waiting on a socket that is
    // gone. A write for it that turns up later - an auth step still awaiting
    // the password function, say - would throw in nextWrite, outside any
    // promise, and take the process down.
    if (!socket)
      return false

    chunk = chunk ? Buffer.concat([chunk, x]) : Buffer.from(x)
    if (fn || chunk.length >= 1024)
      return nextWrite(fn)
    nextWriteTimer === null && (nextWriteTimer = setImmediate(nextWrite))
    return true
  }

  function nextWrite(fn) {
    const x = socket.write(chunk, fn)
    nextWriteTimer !== null && clearImmediate(nextWriteTimer)
    chunk = nextWriteTimer = null
    return x
  }

  function connectTimedOut() {
    errored(Errors.connection('CONNECT_TIMEOUT', options, socket))
    socket.destroy()
  }

  async function secure() {
    if (sslnegotiation !== 'direct') {
      write(SSLRequest)
      const canSSL = await new Promise(r => socket.once('data', x => r(x[0] === 83))) // S

      if (!canSSL && ssl === 'prefer')
        return connected()
    }

    const options = {
      socket,
      servername: net.isIP(socket.host) ? undefined : socket.host
    }

    if (sslnegotiation === 'direct')
      options.ALPNProtocols = ['postgresql']

    if (ssl === 'require' || ssl === 'allow' || ssl === 'prefer')
      options.rejectUnauthorized = false
    else if (typeof ssl === 'object')
      Object.assign(options, ssl)

    socket.removeAllListeners()
    socket = tls.connect(options)
    socket.on('secureConnect', connected)
    socket.on('error', socketError)
    socket.on('close', closed)
    socket.on('drain', drain)
  }

  /* c8 ignore next 3 */
  function drain() {
    !query && onopen(connection)
  }

  function data(x) {
    if (incomings) {
      incomings.push(x)
      remaining -= x.length
      if (remaining > 0)
        return
    }

    incoming = incomings
      ? Buffer.concat(incomings, length - remaining)
      : incoming.length === 0
        ? x
        : Buffer.concat([incoming, x], incoming.length + x.length)

    while (incoming.length > 4) {
      length = incoming.readUInt32BE(1)
      if (length >= incoming.length) {
        remaining = length - incoming.length
        incomings = [incoming]
        break
      }

      try {
        handle(incoming.subarray(0, length + 1))
      } catch (e) {
        query && (query.cursorFn || query.describeFirst) && write(Sync)
        errored(e)
      }
      incoming = incoming.subarray(length + 1)
      remaining = 0
      incomings = null
    }
  }

  async function connect() {
    terminated = false
    idleFatal = null
    backendParameters = {}
    socket || (socket = await createSocket())

    if (!socket)
      return

    connectTimer.start()

    if (options.socket)
      return ssl ? secure() : connected()

    socket.on('connect', ssl ? secure : connected)

    if (options.path)
      return socket.connect(options.path)

    socket.ssl = ssl
    socket.connect(port[hostIndex], host[hostIndex])
    socket.host = host[hostIndex]
    socket.port = port[hostIndex]

    hostIndex = (hostIndex + 1) % port.length
  }

  function reconnect() {
    setTimeout(connect, closedTime ? Math.max(0, closedTime + delay - now()) : 0)
  }

  function connected() {
    try {
      statements = {}
      needsTypes = options.fetch_types
      statementId = Math.random().toString(36).slice(2)
      statementCount = 1
      lifeTimer.start()
      socket.on('data', data)
      keep_alive && socket.setKeepAlive && socket.setKeepAlive(true, 1000 * keep_alive)
      const s = StartupMessage()
      write(s)
    } catch (err) {
      error(err)
    }
  }

  // A socket error that follows the server's own error is the server's error
  // as far as the queries it fails are concerned: a FATAL such as 57P01 is
  // often followed by a reset, because the Sync a cursor answers any error
  // with reaches a backend that has already exited.
  function socketError(err) {
    error(errorResponse || err)
  }

  function error(err) {
    if (connection.queue === queues.connecting && options.host[retries + 1])
      return

    unran(err)
    errored(err)
    while (sent.length)
      queryError(sent.shift(), err)
  }

  function errored(err) {
    stream && (stream.destroy(err), stream = null)
    query && queryError(query, err)
    initial && (queryError(initial, err), initial = null)
  }

  // Postgres ends an idle session with 57P05 (idle_session_timeout) only while
  // nothing is in flight on it. A statement answered with it was written after
  // the server had already ended the session - a process frozen past the
  // timeout (a laptop's sleep, a serverless thaw) writes its first statement
  // to the reaped connection - so it never ran. Outside a reserved scope
  // (begin, reserve: the session, and its transaction or state, are gone with
  // it) each such statement goes back to the pool once, for a fresh
  // connection, instead of failing. A cursor is left to fail, and so is a
  // statement already sent again. They are handed over once the socket is
  // gone (resend), never to this connection.
  function unran(err) {
    if (!onresend || !err || err.code !== '57P05' || connection.reserved || stream)
      return

    query && resendable(query) && (again.push(query), query = null)
    for (let i = sent.length; i > 0; i--) {
      const q = sent.shift()
      resendable(q) ? again.push(q) : sent.push(q)
    }
  }

  function resendable(q) {
    return !q.resent && !q.cursorFn && !q.cancelled
  }

  function resend() {
    const xs = again
    again = []
    xs.forEach(q => {
      q.resent = true
      // Rebuilt from its arguments: Bind serialized the parameters in place.
      q.parameters = null
      q.bound = false
      q.active = false
      onresend(q)
    })
  }

  function queryError(query, err) {
    if (query.reserve)
      return query.reject(err)

    if (!err || typeof err !== 'object')
      err = new Error(err)

    'query' in err || 'parameters' in err || Object.defineProperties(err, {
      stack: { value: err.stack + query.origin.replace(/.*\n/, '\n'), enumerable: options.debug },
      query: { value: query.string, enumerable: options.debug },
      parameters: { value: query.parameters, enumerable: options.debug },
      args: { value: query.args, enumerable: options.debug },
      types: { value: query.statement && query.statement.types, enumerable: options.debug }
    })
    query.reject(err)
  }

  function end() {
    return ending || (
      !connection.reserved && onend(connection),
      !connection.reserved && !initial && !query && sent.length === 0
        ? (terminate(), new Promise(r => socket && socket.readyState !== 'closed' ? socket.once('close', r) : r()))
        : ending = new Promise(r => ended = r)
    )
  }

  function terminate() {
    terminated = true
    if (stream || query || initial || sent.length)
      error(Errors.connection('CONNECTION_DESTROYED', options))

    clearImmediate(nextWriteTimer)
    if (socket) {
      socket.removeListener('data', data)
      socket.removeListener('connect', connected)
      socket.readyState === 'open' && socket.end(b().X().end())
    }
    ended && (ended(), ending = ended = null)
  }

  async function closed(hadError) {
    incoming = Buffer.alloc(0)
    remaining = 0
    incomings = null
    clearImmediate(nextWriteTimer)
    chunk = nextWriteTimer = null
    socket.removeListener('data', data)
    socket.removeListener('connect', connected)
    idleTimer.cancel()
    lifeTimer.cancel()
    connectTimer.cancel()

    socket.removeAllListeners()
    socket = null
    refed = true

    // Whatever the socket still owed a reply fails with it, with the server's
    // own error when one arrived: a FATAL such as 57P01 waits for a
    // ReadyForQuery that never comes. That includes work sent since an 'error'
    // event, and a multi-host connection still in `connecting`, which error()
    // spares so that the next host can be tried - a query already sent can't be.
    if (!initial && (query || sent.length)) {
      const err = errorResponse || lostError || Errors.connection('CONNECTION_CLOSED', options, socket)
      unran(err)
      errored(err)
      while (sent.length)
        queryError(sent.shift(), err)
    }

    // The rest of the socket's state dies with it. Kept, the reopened socket's
    // first ReadyForQuery hands the dead one's error, or its query, to the next
    // query in line, and a half-read row stream offsets that query's rows. A
    // socket that closes while starting up is simply reopened: all it owed was
    // the startup's own internal queries.
    query = results = errorResponse = lostError = null
    result = new Result()
    rows = 0

    if (initial)
      return (resend(), reconnect())

    closedTime = now()
    hadError && options.shared.retries++
    delay = (typeof backoff === 'function' ? backoff(options.shared.retries) : backoff) * 1000
    onclose(connection, idleFatal || Errors.connection('CONNECTION_CLOSED', options, socket))
    // After onclose: the pool has moved this connection out of `busy`, so
    // none of them can be pipelined back onto the socket that is gone.
    resend()
    // An end() awaited while this connection was still connecting (or busy)
    // is only ever settled by terminate(); a socket that dies with nothing
    // left to do must settle it too, or sql.end() hangs forever.
    !initial && ended && (ended(), ending = ended = null)
  }

  /* Handlers */
  function handle(xs, x = xs[0]) {
    (
      x === 68 ? DataRow :                   // D
      x === 100 ? CopyData :                 // d
      x === 65 ? NotificationResponse :      // A
      x === 83 ? ParameterStatus :           // S
      x === 90 ? ReadyForQuery :             // Z
      x === 67 ? CommandComplete :           // C
      x === 50 ? BindComplete :              // 2
      x === 49 ? ParseComplete :             // 1
      x === 116 ? ParameterDescription :     // t
      x === 84 ? RowDescription :            // T
      x === 82 ? Authentication :            // R
      x === 110 ? NoData :                   // n
      x === 75 ? BackendKeyData :            // K
      x === 69 ? ErrorResponse :             // E
      x === 115 ? PortalSuspended :          // s
      x === 51 ? CloseComplete :             // 3
      x === 71 ? CopyInResponse :            // G
      x === 78 ? NoticeResponse :            // N
      x === 72 ? CopyOutResponse :           // H
      x === 99 ? CopyDone :                  // c
      x === 73 ? EmptyQueryResponse :        // I
      x === 86 ? FunctionCallResponse :      // V
      x === 118 ? NegotiateProtocolVersion : // v
      x === 87 ? CopyBothResponse :          // W
      /* c8 ignore next */
      UnknownMessage
    )(xs)
  }

  function DataRow(x) {
    let index = 7
    let length
    let column
    let value

    const row = query.isRaw ? new Array(query.statement.columns.length) : {}
    for (let i = 0; i < query.statement.columns.length; i++) {
      column = query.statement.columns[i]
      length = x.readInt32BE(index)
      index += 4

      value = length === -1
        ? null
        : query.isRaw === true
          ? x.subarray(index, index += length)
          : column.parser === undefined
            ? x.toString('utf8', index, index += length)
            : column.parser.array === true
              ? column.parser(x.toString('utf8', index + 1, index += length))
              : column.parser(x.toString('utf8', index, index += length))

      query.isRaw
        ? (row[i] = query.isRaw === true
          ? value
          : transform.value.from ? transform.value.from(value, column) : value)
        : (row[column.name] = transform.value.from ? transform.value.from(value, column) : value)
    }

    query.forEachFn
      ? query.forEachFn(transform.row.from ? transform.row.from(row) : row, result)
      : (result[rows++] = transform.row.from ? transform.row.from(row) : row)
  }

  function ParameterStatus(x) {
    const [k, v] = x.toString('utf8', 5, x.length - 1).split(b.N)
    backendParameters[k] = v
    if (options.parameters[k] !== v) {
      options.parameters[k] = v
      onparameter && onparameter(k, v)
    }
  }

  function ReadyForQuery(x) {
    if (query) {
      if (errorResponse) {
        query.guessed && !query.bound && described(options).delete(query.described)
        query.retried
          ? errored(Object.assign(errorResponse, { cause: query.retried }))
          : query.prepared && retryRoutines.has(errorResponse.routine)
            ? retry(query, errorResponse)
            // Parameter types reused from an earlier description that no longer
            // fit (the schema changed under the text): nothing ran, so outside a
            // transaction it is safe to describe afresh and run it again.
            : query.guessed && !query.bound && x[5] === 73 // I
              ? (query.parameters = null, retry(query, errorResponse))
              : errored(errorResponse)
      } else {
        query.resolve(results || result)
      }
    } else if (errorResponse) {
      errored(errorResponse)
    }

    // rows too: only CommandComplete resets it, and a query that failed partway
    // through its rows never gets one - the next query's rows would start
    // where that one's stopped, leaving holes at the front of its result.
    query = results = errorResponse = null
    result = new Result()
    rows = 0
    connectTimer.cancel()

    if (initial) {
      if (target_session_attrs) {
        if (!backendParameters.in_hot_standby || !backendParameters.default_transaction_read_only)
          return fetchState()
        else if (tryNext(target_session_attrs, backendParameters))
          return terminate()
      }

      if (needsTypes) {
        initial.reserve && (initial = null)
        return fetchArrayTypes()
      }

      initial && !initial.reserve && execute(initial)
      options.shared.retries = retries = 0
      initial = null

      // execute() writes nothing for a query cancelled before it reached the
      // wire, and isn't called at all for a reserve, so no further
      // ReadyForQuery is coming. Fall through with nothing outstanding, or the
      // connection strands in `connecting` — a lost pool slot, and a parked
      // end() that only terminate() would have settled.
      if (query || sent.length)
        return
    }

    while (sent.length && (query = sent.shift()) && (start(query), query.cancelled)) {
      Connection(options).cancel(query.state, query.cancelled.resolve, query.cancelled.reject)
      unanswered(query)
    }

    if (query)
      return // Consider opening if able and sent.length < 50

    connection.reserved
      ? !connection.reserved.release && x[5] === 73 // I
        ? ending
          ? terminate()
          : (connection.reserved = null, onopen(connection))
        : connection.reserved()
      : ending
        ? onending(connection) || terminate()
        : onopen(connection)
  }

  function CommandComplete(x) {
    rows = 0

    for (let i = x.length - 1; i > 0; i--) {
      if (x[i] === 32 && x[i + 1] < 58 && result.count === null)
        result.count = +x.toString('utf8', i + 1, x.length - 1)
      if (x[i - 1] >= 65) {
        result.command = x.toString('utf8', 5, i)
        result.state = backend
        break
      }
    }

    final && (final(), final = null)

    if (result.command === 'BEGIN' && max !== 1 && !connection.reserved)
      return errored(Errors.generic('UNSAFE_TRANSACTION', 'Only use sql.begin, sql.reserved or max: 1'))

    if (query.options.simple)
      return BindComplete()

    if (query.cursorFn) {
      result.count && query.cursorFn(result)
      write(Sync)
    }
  }

  function ParseComplete() {
    query.parsing = false
  }

  function BindComplete() {
    query.bound = true
    !result.statement && (result.statement = query.statement)
    result.columns = query.statement.columns
  }

  function ParameterDescription(x) {
    const length = x.readUInt16BE(5)

    for (let i = 0; i < length; ++i)
      !query.statement.types[i] && (query.statement.types[i] = x.readUInt32BE(7 + i * 4))

    query.described && !query.guessed && learn(options, query.described, query.statement.types)
    query.prepare && (statements[query.signature] = query.statement)
    query.describeFirst && !query.onlyDescribe && (write(prepared(query)), query.describeFirst = false)
  }

  function RowDescription(x) {
    if (result.command) {
      results = results || [result]
      results.push(result = new Result())
      result.count = null
      query.statement.columns = null
    }

    const length = x.readUInt16BE(5)
    let index = 7
    let start

    query.statement.columns = Array(length)

    for (let i = 0; i < length; ++i) {
      start = index
      while (x[index++] !== 0);
      const table = x.readUInt32BE(index)
      const number = x.readUInt16BE(index + 4)
      const type = x.readUInt32BE(index + 6)
      query.statement.columns[i] = {
        name: transform.column.from
          ? transform.column.from(x.toString('utf8', start, index - 1))
          : x.toString('utf8', start, index - 1),
        parser: parsers[type],
        table,
        number,
        type
      }
      index += 18
    }

    result.statement = query.statement
    if (query.onlyDescribe)
      return (query.resolve(query.statement), write(Sync))
  }

  async function Authentication(x, type = x.readUInt32BE(5)) {
    (
      type === 3 ? AuthenticationCleartextPassword :
      type === 5 ? AuthenticationMD5Password :
      type === 10 ? SASL :
      type === 11 ? SASLContinue :
      type === 12 ? SASLFinal :
      type !== 0 ? UnknownAuth :
      noop
    )(x, type)
  }

  /* c8 ignore next 5 */
  async function AuthenticationCleartextPassword() {
    const payload = await Pass()
    write(
      b().p().str(payload).z(1).end()
    )
  }

  async function AuthenticationMD5Password(x) {
    const payload = 'md5' + (
      await md5(
        Buffer.concat([
          Buffer.from(await md5((await Pass()) + user)),
          x.subarray(9)
        ])
      )
    )
    write(
      b().p().str(payload).z(1).end()
    )
  }

  async function SASL() {
    nonce = (await crypto.randomBytes(18)).toString('base64')
    b().p().str('SCRAM-SHA-256' + b.N)
    const i = b.i
    write(b.inc(4).str('n,,n=*,r=' + nonce).i32(b.i - i - 4, i).end())
  }

  async function SASLContinue(x) {
    const res = x.toString('utf8', 9).split(',').reduce((acc, x) => (acc[x[0]] = x.slice(2), acc), {})

    const saltedPassword = await crypto.pbkdf2Sync(
      await Pass(),
      Buffer.from(res.s, 'base64'),
      parseInt(res.i), 32,
      'sha256'
    )

    const clientKey = await hmac(saltedPassword, 'Client Key')

    const auth = 'n=*,r=' + nonce + ','
               + 'r=' + res.r + ',s=' + res.s + ',i=' + res.i
               + ',c=biws,r=' + res.r

    serverSignature = (await hmac(await hmac(saltedPassword, 'Server Key'), auth)).toString('base64')

    const payload = 'c=biws,r=' + res.r + ',p=' + xor(
      clientKey, Buffer.from(await hmac(await sha256(clientKey), auth))
    ).toString('base64')

    write(
      b().p().str(payload).end()
    )
  }

  function SASLFinal(x) {
    if (x.toString('utf8', 9).split(b.N, 1)[0].slice(2) === serverSignature)
      return
    /* c8 ignore next 5 */
    errored(Errors.generic('SASL_SIGNATURE_MISMATCH', 'The server did not return the correct signature'))
    socket.destroy()
  }

  function Pass() {
    return Promise.resolve(typeof options.pass === 'function'
      ? options.pass()
      : options.pass
    )
  }

  function NoData() {
    result.statement = query.statement
    result.statement.columns = []
    if (query.onlyDescribe)
      return (query.resolve(query.statement), write(Sync))
  }

  function BackendKeyData(x) {
    backend.pid = x.readUInt32BE(5)
    backend.secret = x.readUInt32BE(9)
  }

  async function fetchArrayTypes() {
    needsTypes = false
    const types = await new Query([`
      select b.oid, b.typarray
      from pg_catalog.pg_type a
      left join pg_catalog.pg_type b on b.oid = a.typelem
      where a.typcategory = 'A'
      group by b.oid, b.typarray
      order by b.oid
    `], [], execute)
    types.forEach(({ oid, typarray }) => addArrayType(oid, typarray))
  }

  function addArrayType(oid, typarray) {
    if (!!options.parsers[typarray] && !!options.serializers[typarray]) return
    const parser = options.parsers[oid]
    options.shared.typeArrayMap[oid] = typarray
    options.parsers[typarray] = (xs) => arrayParser(xs, parser, typarray)
    options.parsers[typarray].array = true
    options.serializers[typarray] = (xs) => arraySerializer(xs, options.serializers[oid], options, typarray)
  }

  function tryNext(x, xs) {
    return (
      (x === 'read-write' && xs.default_transaction_read_only === 'on') ||
      (x === 'read-only' && xs.default_transaction_read_only === 'off') ||
      (x === 'primary' && xs.in_hot_standby === 'on') ||
      (x === 'standby' && xs.in_hot_standby === 'off') ||
      (x === 'prefer-standby' && xs.in_hot_standby === 'off' && options.host[retries])
    )
  }

  function fetchState() {
    const query = new Query([`
      show transaction_read_only;
      select pg_catalog.pg_is_in_recovery()
    `], [], execute, null, { simple: true })
    query.resolve = ([[a], [b]]) => {
      backendParameters.default_transaction_read_only = a.transaction_read_only
      backendParameters.in_hot_standby = b.pg_is_in_recovery ? 'on' : 'off'
    }
    query.execute()
  }

  function ErrorResponse(x) {
    if (query) {
      (query.cursorFn || query.describeFirst) && write(Sync)
      errorResponse = Errors.postgres(parseError(x))
    } else {
      // Nothing in flight: a FATAL the server sends an idle session
      // (idle_in_transaction_session_timeout's 25P03, idle_session_timeout's
      // 57P05, an administrator's terminate) is the reason the socket is about
      // to close. Kept, it is what the close reports (a transaction or a
      // reserved connection, which cannot reopen, fails with it) and what the
      // next query on this connection is rejected with, instead of a bare
      // CONNECTION_CLOSED.
      const err = Errors.postgres(parseError(x))
      if (err.severity === 'FATAL' || err.severity === 'PANIC')
        idleFatal = err
      errored(err)
    }
  }

  // A cached plan the server invalidated is re-prepared and run again. If
  // that second run fails, the error to report is the one it hit — not the
  // stale-plan error that caused the retry, which is an internal detail the
  // caller did not cause. The trigger stays reachable as `cause`.
  // The query is now the one this connection's backend works on: written
  // straight to an idle connection, or next in line once the statement it was
  // pipelined behind completed. startedAt marks that moment, so a caller can
  // tell the time a statement spent on the server from the time it waited for
  // a connection or behind another statement. A retry keeps the first mark.
  function start(q) {
    q.active = true
    q.connection = connection
    q.startedAt || (q.startedAt = now())
  }

  function retry(q, error) {
    delete statements[q.signature]
    // The same text's reusable description is as stale as the plan was (the
    // signature is the description's key, types + text).
    q.signature && described(options).delete(q.signature)
    q.retried = error
    execute(q)
  }

  function NotificationResponse(x) {
    if (!onnotify)
      return

    let index = 9
    while (x[index++] !== 0);
    onnotify(
      x.toString('utf8', 9, index - 1),
      x.toString('utf8', index, x.length - 1)
    )
  }

  async function PortalSuspended() {
    const q = query
    try {
      const x = await Promise.resolve(q.cursorFn(result))
      // The connection closed while the cursor's consumer had the batch: q
      // failed with it, and whatever is current now - on a reopened socket -
      // is some other query's, so neither the connection's state nor its
      // socket is q's to touch.
      if (q !== query)
        return
      rows = 0
      x === CLOSE
        ? write(Close(q.portal))
        : (result = new Result(), write(Execute('', q.cursorRows)))
    } catch (err) {
      q === query && write(Sync)
      q.reject(err)
    }
  }

  function CloseComplete() {
    result.count && query.cursorFn(result)
    query.resolve(result)
  }

  function CopyInResponse() {
    stream = new Stream.Writable({
      autoDestroy: true,
      write(chunk, encoding, callback) {
        socket.write(b().d().raw(chunk).end(), callback)
      },
      destroy(error, callback) {
        callback(error)
        // The socket may be gone already: closed() destroys the stream after
        // the socket closed under it.
        socket && socket.write(b().f().str(error + b.N).end())
        stream = null
      },
      final(callback) {
        socket.write(b().c().end())
        final = callback
        stream = null
      }
    })
    query.resolve(stream)
  }

  function CopyOutResponse() {
    stream = new Stream.Readable({
      read() { socket.resume() }
    })
    query.resolve(stream)
  }

  /* c8 ignore next 3 */
  function CopyBothResponse() {
    stream = new Stream.Duplex({
      autoDestroy: true,
      read() { socket.resume() },
      /* c8 ignore next 11 */
      write(chunk, encoding, callback) {
        socket.write(b().d().raw(chunk).end(), callback)
      },
      destroy(error, callback) {
        callback(error)
        // The socket may be gone already: closed() destroys the stream after
        // the socket closed under it.
        socket && socket.write(b().f().str(error + b.N).end())
        stream = null
      },
      final(callback) {
        socket.write(b().c().end())
        final = callback
      }
    })
    // The replication stream's watchdog (subscribe.js) needs the connection
    // under the stream gone when the stream stops delivering: ending the
    // stream alone would leave its half-open socket holding the pool's one
    // connection, and the re-established subscription would queue behind it.
    stream.lose = lose
    query.resolve(stream)
  }

  function CopyData(x) {
    stream && (stream.push(x.subarray(5)) || socket.pause())
  }

  function CopyDone() {
    stream && stream.push(null)
    stream = null
  }

  function NoticeResponse(x) {
    onnotice
      ? onnotice(parseError(x))
      : console.log(parseError(x)) // eslint-disable-line

  }

  /* c8 ignore next 3 */
  function EmptyQueryResponse() {
    /* noop */
  }

  /* c8 ignore next 3 */
  function FunctionCallResponse() {
    errored(Errors.notSupported('FunctionCallResponse'))
  }

  /* c8 ignore next 3 */
  function NegotiateProtocolVersion() {
    errored(Errors.notSupported('NegotiateProtocolVersion'))
  }

  /* c8 ignore next 3 */
  function UnknownMessage(x) {
    console.error('Postgres.js : Unknown Message:', x[0]) // eslint-disable-line
  }

  /* c8 ignore next 3 */
  function UnknownAuth(x, type) {
    console.error('Postgres.js : Unknown Auth:', type) // eslint-disable-line
  }

  /* Messages */
  function Bind(parameters, types, statement = '', portal = '') {
    let prev
      , type

    b().B().str(portal + b.N).str(statement + b.N).i16(0).i16(parameters.length)

    parameters.forEach((x, i) => {
      if (x === null)
        return b.i32(0xFFFFFFFF)

      type = types[i]
      parameters[i] = x = type in options.serializers
        ? options.serializers[type](x)
        : '' + x

      prev = b.i
      b.inc(4).str(x).i32(b.i - prev - 4, prev)
    })

    b.i16(0)

    return b.end()
  }

  function Parse(str, parameters, types, name = '') {
    b().P().str(name + b.N).str(str + b.N).i16(parameters.length)
    parameters.forEach((x, i) => b.i32(types[i] || 0))
    return b.end()
  }

  function Describe(x, name = '') {
    return b().D().str(x).str(name + b.N).end()
  }

  function Execute(portal = '', rows = 0) {
    return Buffer.concat([
      b().E().str(portal + b.N).i32(rows).end(),
      Flush
    ])
  }

  function Close(portal = '') {
    return Buffer.concat([
      b().C().str('P').str(portal + b.N).end(),
      b().S().end()
    ])
  }

  function StartupMessage() {
    return cancelMessage || b().inc(4).i16(3).z(2).str(
      Object.entries(Object.assign({
        user,
        database,
        client_encoding: 'UTF8'
      },
        options.connection
      )).filter(([, v]) => v).map(([k, v]) => k + b.N + v).join(b.N)
    ).z(2).end(0)
  }

}

function parseError(x) {
  const error = {}
  let start = 5
  for (let i = 5; i < x.length - 1; i++) {
    if (x[i] === 0) {
      error[errorFields[x[start]]] = x.toString('utf8', start + 1, i)
      start = i + 1
    }
  }
  return error
}

function md5(x) {
  return crypto.createHash('md5').update(x).digest('hex')
}

function hmac(key, x) {
  return crypto.createHmac('sha256', key).update(x).digest()
}

function sha256(x) {
  return crypto.createHash('sha256').update(x).digest()
}

function xor(a, b) {
  const length = Math.max(a.length, b.length)
  const buffer = Buffer.allocUnsafe(length)
  for (let i = 0; i < length; i++)
    buffer[i] = a[i] ^ b[i]
  return buffer
}

function timer(fn, seconds) {
  seconds = typeof seconds === 'function' ? seconds() : seconds
  if (!seconds)
    return { cancel: noop, start: noop }

  let timer
  return {
    cancel() {
      timer && (clearTimeout(timer), timer = null)
    },
    start() {
      timer && clearTimeout(timer)
      timer = setTimeout(done, seconds * 1000, arguments)
      // Idle and lifetime timers only close a connection: they never hold
      // the process open themselves.
      timer && timer.unref && timer.unref()
    }
  }

  function done(args) {
    fn.apply(null, args)
    timer = null
  }
}
