const noop = () => { /* noop */ }

export default function Subscribe(postgres, options) {
  const subscribers = new Map()
      , state = {}
      , hwm = options.subscribe_high_water_mark || 1024
      , lwm = Math.ceil(hwm / 4)

  let connection
    , stream
    , flush
    , durable = !!options.slot
    , slot = options.slot || 'postgresjs_' + Math.random().toString(36).slice(2)
    , ended = false
    , reconnecting = false

  const sql = subscribe.sql = postgres({
    ...options,
    transform: { column: {}, value: {}, row: {} },
    max: 1,
    fetch_types: false,
    idle_timeout: null,
    max_lifetime: null,
    connection: {
      ...options.connection,
      replication: 'database'
    },
    onclose: reestablish,
    no_subscribe: true
  })

  const end = sql.end
      , close = sql.close

  sql.end = async(...xs) => {
    ended = true
    await endStream()
    return end(...xs)
  }

  sql.close = async(...xs) => {
    await endStream()
    return close(...xs)
  }

  // Idempotent: the outer sql.end() ends the subscription pool too, so this
  // runs twice on a normal teardown - waiting for 'close' on an already closed
  // stream never returns.
  function endStream() {
    const s = stream
    stream = null
    flush && flush()
    flush = null
    return s && !s.destroyed && !s.writableEnded
      ? new Promise(r => (s.once('close', r), s.end()))
      : Promise.resolve()
  }

  return subscribe

  // Serialized, guarded re-establishment, entered from both the connection's
  // onclose and the replication stream's close. It must never reject (it is
  // invoked without a handler — an unhandled rejection kills Node processes
  // by default), and attempts genuinely can fail: a terminated walsender's
  // late ErrorResponse rejects the freshly queued slot query. Retry with
  // backoff until the stream is back or the instance ended; a fresh slot
  // name per attempt sidesteps lingering TEMPORARY slots when the session
  // survived the stream. A durable (named) slot keeps its name — that is the
  // whole point: the server retained the WAL and we resume from the last
  // confirmed LSN.
  async function reestablish() {
    if (ended || reconnecting)
      return
    reconnecting = true
    stream = null
    state.pid = state.secret = undefined
    try {
      for (let attempt = 0; !ended; attempt++) {
        try {
          durable || (slot = 'postgresjs_' + Math.random().toString(36).slice(2))
          connected(await init(sql, slot, options.publications))
          subscribers.forEach(event => event.forEach(({ onsubscribe }) => onsubscribe()))
          break
        } catch (error) {
          subscribers.forEach(event => event.forEach(({ onerror }) => onerror(error)))
          await new Promise(r => setTimeout(r, Math.min(1000, 50 << attempt)))
        }
      }
    } finally {
      reconnecting = false
    }
  }

  async function subscribe(event, fn, onsubscribe = noop, onerror = noop, subscribeOptions) {
    event = parseEvent(event)
    subscribeOptions && subscribeOptions.slot !== undefined && useSlot(subscribeOptions.slot)
    durable && validateSlot(slot)

    if (!connection)
      connection = init(sql, slot, options.publications)

    const subscriber = { fn, onsubscribe, onerror }
    const fns = subscribers.has(event)
      ? subscribers.get(event).add(subscriber)
      : subscribers.set(event, new Set([subscriber])).get(event)

    const unsubscribe = () => {
      fns.delete(subscriber)
      fns.size === 0 && subscribers.delete(event)
    }

    return connection.then(x => {
      connected(x)
      onsubscribe()
      stream && stream.on('error', onerror)
      return { unsubscribe, drop, state, sql, get slot() { return slot } }
    })
  }

  // The slot belongs to the subscription's single replication connection, so
  // every subscriber on the same sql shares it — naming it twice is fine,
  // renaming it is not.
  function useSlot(name) {
    validateSlot(name)
    if (connection && name !== slot)
      throw new Error('The replication slot is already ' + slot + ' - it cannot be changed while subscribed')
    slot = name
    durable = true
  }

  function validateSlot(name) {
    if (typeof name !== 'string' || !/^[a-z0-9_]{1,63}$/.test(name))
      throw new Error('Invalid replication slot name: ' + name + ' - use lowercase letters, digits and underscores (max 63)')
  }

  // Ends the subscription and removes the slot server-side. Plain sql.end()
  // deliberately leaves a durable slot behind (that is what makes it durable),
  // so this is the only way to stop paying its WAL retention.
  async function drop() {
    ended = true
    await endStream()
    for (let attempt = 0; ; attempt++) {
      try {
        await sql.unsafe(`DROP_REPLICATION_SLOT ${ slot } WAIT`)
        break
      } catch (error) {
        // 55006 object_in_use - the walsender has not released the slot yet
        if (error.code !== '55006' || attempt >= 10)
          throw error
        await new Promise(r => setTimeout(r, 100))
      }
    }
    return end()
  }

  function connected(x) {
    stream = x.stream
    flush = x.flush
    state.pid = x.state.pid
    state.secret = x.state.secret
  }

  async function init(sql, slot, publications) {
    if (!publications)
      throw new Error('Missing publication names')

    const xs = durable
      ? await durableSlot(sql, slot)
      : await sql.unsafe(
        `CREATE_REPLICATION_SLOT ${ slot } TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT`
      )

    const [x] = xs

    const v2 = parseInt(sql.parameters.server_version) >= 14

    // 0/0 tells the server to resume the durable slot from its confirmed_flush_lsn.
    const stream = await sql.unsafe(
      `START_REPLICATION SLOT ${ slot } LOGICAL ${
        durable ? '0/0' : x.consistent_point
      } (proto_version '${ v2 ? '2' : '1' }', publication_names '${ publications }'${ v2 ? ', streaming \'on\'' : '' })`
    ).writable()

    const state = {
      lsn: lsn(durable ? x.lsn : x.consistent_point)
    }

    const live = new Set()
        , txs = new Map()
        , unacked = []

    let tx = null
      , begun = null
      , queued = 0
      , paused = false
      , heartbeat = null
      , acking = null
      , acked = state.lsn

    stream.on('data', data)
    stream.on('error', error)
    stream.on('close', teardown)
    stream.on('close', reestablish)

    return { stream, state: xs.state, flush }

    // Send a scheduled ack now rather than losing it to the close - anything
    // acked but unconfirmed would simply be redelivered on the next connect.
    function flush() {
      if (!acking)
        return
      clearTimeout(acking)
      acking = null
      stream.destroyed || stream.writableEnded || pong()
    }

    function error(e) {
      console.error('Unexpected error during logical streaming - reconnecting', e) // eslint-disable-line
    }

    function data(x) {
      if (x[0] === 0x77) {
        parse(x.subarray(25), state, sql.options.parsers, handle, options.transform)
      } else if (x[0] === 0x6b) {
        // Nothing decoded is outstanding, so the server's walEnd is safe to confirm -
        // without this the slot would pin the WAL of every unpublished write.
        durable && !unacked.length && !begun && !txs.size && (acked = Buffer.from(x.subarray(1, 9)))
        if (x[17]) {
          state.lsn = x.subarray(1, 9)
          pong()
        }
      }
    }

    function handle(a, b) {
      b.command === 'begin' ? begin(b)
        : b.command === 'commit' ? commit(b)
        : b.command === 'stream_commit' ? streamCommit(b)
        : b.command === 'stream_abort' ? streamAbort(b)
        : state.stream ? streamed(a, b)
        : row(a, b)
    }

    function begin(b) {
      begun = { xid: b.xid, streaming: false, lsn: null, date: null }
      tx = null
    }

    function commit(b) {
      tx && (tx.info.lsn = b.lsn, tx.info.date = b.date, tx.end())
      track(tx, b.end)
      tx = begun = null
    }

    function streamCommit(b) {
      const t = txs.get(b.xid)
      txs.delete(b.xid)
      t && (t.info.lsn = b.lsn, t.info.date = b.date, t.end())
      track(t, b.end)
    }

    function streamAbort(b) {
      if (b.subxid === b.xid) {
        const t = txs.get(b.xid)
        txs.delete(b.xid)
        t && t.error(Object.assign(new Error('Transaction ' + b.xid + ' aborted'), { xid: b.xid }))
      } else {
        const t = txs.get(b.xid)
        t && t.push({ command: 'abort', xid: b.subxid })
      }
    }

    function streamed(a, b) {
      let t = txs.get(state.stream)
      t === undefined && txs.set(state.stream, t = transaction({ xid: state.stream, streaming: true, lsn: null, date: null }))
      t && t.push(change(a, b))
    }

    function row(a, b) {
      b.command === 'truncate' || dispatch(a, b)
      if (begun) {
        tx === null && (tx = transaction(begun))
        tx && tx.push(change(a, b))
      }
    }

    function change(a, b) {
      return b.command === 'truncate'
        ? { command: 'truncate', relations: b.relations, cascade: b.cascade, restartIdentity: b.restartIdentity, xid: b.xid }
        : { command: b.command, row: a, old: b.old || null, relation: b.relation, xid: b.xid }
    }

    function dispatch(a, b) {
      const path = b.relation.schema + '.' + b.relation.table
      call('*', a, b)
      call('*:' + path, a, b)
      b.relation.keys.length && call('*:' + path + '=' + b.relation.keys.map(x => a[x.name]), a, b)
      call(b.command, a, b)
      call(b.command + ':' + path, a, b)
      b.relation.keys.length && call(b.command + ':' + path + '=' + b.relation.keys.map(x => a[x.name]), a, b)
    }

    function transaction(info) {
      const fns = subscribers.get('transaction')
      if (!fns || fns.size === 0)
        return false

      const t = {
        info,
        iterators: [],
        waiting: 0,
        failed: false,
        acked: false,
        onack: null,
        push: x => t.iterators.forEach(it => it.push(x)),
        end: () => (live.delete(t), t.iterators.forEach(it => it.end())),
        error: e => (live.delete(t), t.iterators.forEach(it => it.error(e)))
      }

      info.ack = ack

      // With a durable slot the returned promise is the ack signal: the slot
      // may only advance past this transaction once every handler has settled.
      // A handler that returns nothing cannot be waited for and acks at once.
      fns.forEach(({ fn }) => {
        const it = Changes()
        t.iterators.push(it)
        try {
          const x = fn(it.changes, info, 'transaction')
          if (!x || typeof x.catch !== 'function')
            return
          durable
            ? (t.waiting++, x.then(() => settled(false), e => settled(true, e)))
            : x.catch(error)
        } catch (e) {
          durable ? failed(e) : error(e)
        }
      })

      durable && t.waiting === 0 && !t.failed && ack()

      live.add(t)
      return t

      function settled(rejected, e) {
        rejected && failed(e)
        --t.waiting === 0 && !t.failed && ack()
      }

      // Deliberately does not ack: dropping a transaction the consumer could
      // not handle is worse than a stalled slot, which is at least visible in
      // pg_replication_slots. Call info.ack() to skip it on purpose.
      function failed(e) {
        t.failed = true
        console.error( // eslint-disable-line
          'Transaction handler failed - replication slot ' + slot + ' cannot advance past ' + (t.info.lsn || 'xid ' + t.info.xid),
          e
        )
      }

      function ack() {
        if (t.acked)
          return
        t.acked = true
        t.onack && t.onack()
      }
    }

    // Durable slots only: hold the commit's end LSN until the consumer is done
    // with the transaction, then confirm the longest fully handled prefix.
    // t is false/undefined when nothing was listening - nothing can be lost.
    function track(t, end) {
      if (!durable)
        return

      const entry = { lsn: Buffer.from(end), done: false }
      unacked.push(entry)
      t && !t.acked
        ? (t.onack = () => confirm(entry))
        : confirm(entry)
    }

    function confirm(entry) {
      entry.done = true
      let advanced = false
      while (unacked.length && unacked[0].done)
        acked = unacked.shift().lsn, advanced = true

      if (!advanced || acking)
        return

      acking = setTimeout(() => {
        acking = null
        stream && !stream.destroyed && pong()
      }, 100)
      acking.unref && acking.unref()
    }

    function Changes() {
      const queue = []

      let pending = null
        , done = false
        , failed = null

      return { push, end, error, changes: { [Symbol.asyncIterator]: () => ({ next, return: finish, throw: finish }) } }

      function push(x) {
        if (done)
          return
        if (pending) {
          const p = pending
          pending = null
          p.resolve({ done: false, value: x })
        } else {
          queue.push(x)
          inc(1)
        }
      }

      function end() {
        if (done)
          return
        done = true
        if (pending) {
          const p = pending
          pending = null
          p.resolve({ done: true, value: undefined })
        }
      }

      function error(e) {
        if (done)
          return
        done = true
        failed = e
        dec(queue.length)
        queue.length = 0
        if (pending) {
          const p = pending
          pending = null
          p.reject(e)
        }
      }

      function next() {
        if (queue.length) {
          dec(1)
          return Promise.resolve({ done: false, value: queue.shift() })
        }
        if (failed)
          return Promise.reject(failed)
        if (done)
          return Promise.resolve({ done: true, value: undefined })
        return new Promise((resolve, reject) => pending = { resolve, reject })
      }

      function finish() {
        done = true
        failed = null
        dec(queue.length)
        queue.length = 0
        if (pending) {
          const p = pending
          pending = null
          p.resolve({ done: true, value: undefined })
        }
        return Promise.resolve({ done: true, value: undefined })
      }
    }

    function inc(n) {
      queued += n
      if (!paused && queued >= hwm) {
        paused = true
        stream.pause()
        heartbeat = setInterval(() => stream && !stream.destroyed && pong(), 15000)
        heartbeat.unref && heartbeat.unref()
      }
    }

    function dec(n) {
      queued -= n
      if (paused && queued <= lwm) {
        paused = false
        clearInterval(heartbeat)
        heartbeat = null
        stream.destroyed || stream.resume()
      }
    }

    function teardown() {
      clearInterval(heartbeat)
      clearTimeout(acking)
      heartbeat = acking = null
      paused = false
      queued = 0
      tx = begun = null
      txs.clear()
      unacked.length = 0
      const e = new Error('Subscription stream closed')
      live.forEach(t => t.error(e))
      live.clear()
    }

    function pong() {
      const x = Buffer.alloc(34)
      x[0] = 'r'.charCodeAt(0)
      if (durable) {
        // written / flushed / applied. Only flushed and applied move the slot's
        // confirmed_flush_lsn, so they carry what the consumer has handled.
        const written = Buffer.compare(acked, state.lsn) > 0 ? acked : state.lsn
        written.copy(x, 1)
        acked.copy(x, 9)
        acked.copy(x, 17)
      } else {
        x.fill(state.lsn, 1)
      }
      x.writeBigInt64BE(BigInt(Date.now() - Date.UTC(2000, 0, 1)) * BigInt(1000), 25)
      stream.write(x)
    }
  }

  // Create the named slot unless it is already there - an existing slot is the
  // resume case, not an error. The slot is not TEMPORARY, so the server keeps
  // the WAL for it while we are away. Reads back confirmed_flush_lsn as the
  // point we may not confirm below.
  async function durableSlot(sql, slot) {
    try {
      await sql.unsafe(`CREATE_REPLICATION_SLOT ${ slot } LOGICAL pgoutput NOEXPORT_SNAPSHOT`)
    } catch (error) {
      if (error.code !== '42710') // duplicate_object - ours to resume
        throw error
    }

    const xs = await sql.unsafe(
      `select coalesce(confirmed_flush_lsn, restart_lsn)::text as lsn from pg_replication_slots where slot_name = '${ slot }'`
    )

    if (!xs.length || !xs[0].lsn)
      throw new Error('Replication slot ' + slot + ' has no confirmed position')

    return xs
  }

  function call(x, a, b) {
    subscribers.has(x) && subscribers.get(x).forEach(({ fn }) => fn(a, b, x))
  }
}

function lsn(x) {
  return Buffer.concat(x.split('/').map(x => Buffer.from(('00000000' + x).slice(-8), 'hex')))
}

function Time(x) {
  return new Date(Date.UTC(2000, 0, 1) + Number(x / BigInt(1000)))
}

function Lsn(x, i) {
  return x.readUInt32BE(i).toString(16).toUpperCase() + '/' + x.readUInt32BE(i + 4).toString(16).toUpperCase()
}

function parse(x, state, parsers, handle, transform) {
  const char = (acc, [k, v]) => (acc[k.charCodeAt(0)] = v, acc)

  Object.entries({
    R: x => {  // Relation
      let i = state.stream ? 5 : 1
      const r = state[x.readUInt32BE(i)] = {
        schema: x.toString('utf8', i += 4, i = x.indexOf(0, i)) || 'pg_catalog',
        table: x.toString('utf8', i + 1, i = x.indexOf(0, i + 1)),
        columns: Array(x.readUInt16BE(i += 2)),
        keys: []
      }
      i += 2

      let columnIndex = 0
        , column

      while (i < x.length) {
        column = r.columns[columnIndex++] = {
          key: x[i++],
          name: transform.column.from
            ? transform.column.from(x.toString('utf8', i, i = x.indexOf(0, i)))
            : x.toString('utf8', i, i = x.indexOf(0, i)),
          type: x.readUInt32BE(i += 1),
          parser: parsers[x.readUInt32BE(i)],
          atttypmod: x.readUInt32BE(i += 4)
        }

        column.key && r.keys.push(column)
        i += 4
      }
    },
    Y: () => { /* noop */ }, // Type
    O: () => { /* noop */ }, // Origin
    B: x => { // Begin
      state.date = Time(x.readBigInt64BE(9))
      state.lsn = x.subarray(1, 9)
      state.xid = x.readUInt32BE(17)
      handle(null, { command: 'begin', xid: state.xid })
    },
    I: x => { // Insert
      let i = state.stream ? 5 : 1
      const xid = state.stream ? x.readUInt32BE(1) : state.xid
      const relation = state[x.readUInt32BE(i)]
      const { row } = tuples(x, relation.columns, i += 7, transform)

      handle(row, {
        command: 'insert',
        relation,
        xid
      })
    },
    D: x => { // Delete
      let i = state.stream ? 5 : 1
      const xid = state.stream ? x.readUInt32BE(1) : state.xid
      const relation = state[x.readUInt32BE(i)]
      i += 4
      const key = x[i] === 75
      handle(key || x[i] === 79
        ? tuples(x, relation.columns, i += 3, transform).row
        : null
      , {
        command: 'delete',
        relation,
        key,
        xid
      })
    },
    U: x => { // Update
      let i = state.stream ? 5 : 1
      const xid = state.stream ? x.readUInt32BE(1) : state.xid
      const relation = state[x.readUInt32BE(i)]
      i += 4
      const key = x[i] === 75
      const xs = key || x[i] === 79
        ? tuples(x, relation.columns, i += 3, transform)
        : null

      xs && (i = xs.i)

      const { row } = tuples(x, relation.columns, i + 3, transform)

      handle(row, {
        command: 'update',
        relation,
        key,
        old: xs && xs.row,
        xid
      })
    },
    T: x => { // Truncate
      let i = state.stream ? 5 : 1
      const xid = state.stream ? x.readUInt32BE(1) : state.xid
      const relations = Array(x.readUInt32BE(i))
      const flags = x[i += 4]
      i += 1
      for (let r = 0; r < relations.length; r++) {
        relations[r] = state[x.readUInt32BE(i)]
        i += 4
      }
      handle(null, {
        command: 'truncate',
        relations,
        cascade: !!(flags & 1),
        restartIdentity: !!(flags & 2),
        xid
      })
    },
    S: x => { // Stream Start
      state.stream = x.readUInt32BE(1)
    },
    E: () => { // Stream Stop
      state.stream = null
    },
    c: x => { // Stream Commit
      handle(null, { command: 'stream_commit', xid: x.readUInt32BE(1), lsn: Lsn(x, 6), end: x.subarray(14, 22), date: Time(x.readBigInt64BE(22)) })
    },
    A: x => { // Stream Abort
      handle(null, { command: 'stream_abort', xid: x.readUInt32BE(1), subxid: x.readUInt32BE(5) })
    },
    C: x => { // Commit
      handle(null, { command: 'commit', lsn: Lsn(x, 2), end: x.subarray(10, 18), date: Time(x.readBigInt64BE(18)) })
    }
  }).reduce(char, {})[x[0]](x)
}

function tuples(x, columns, xi, transform) {
  let type
    , column
    , value

  const row = transform.raw ? new Array(columns.length) : {}
  for (let i = 0; i < columns.length; i++) {
    type = x[xi++]
    column = columns[i]
    value = type === 110 // n
      ? null
      : type === 117 // u
        ? undefined
        : column.parser === undefined
          ? x.toString('utf8', xi + 4, xi += 4 + x.readUInt32BE(xi))
          : column.parser.array === true
            ? column.parser(x.toString('utf8', xi + 5, xi += 4 + x.readUInt32BE(xi)))
            : column.parser(x.toString('utf8', xi + 4, xi += 4 + x.readUInt32BE(xi)))

    transform.raw
      ? (row[i] = transform.raw === true
        ? value
        : transform.value.from ? transform.value.from(value, column) : value)
      : (row[column.name] = transform.value.from
        ? transform.value.from(value, column)
        : value
      )
  }

  return { i: xi, row: transform.row.from ? transform.row.from(row) : row }
}

function parseEvent(x) {
  if (/^transaction/i.test(x)) {
    if (!/^transaction$/i.test(x))
      throw new Error('The transaction event does not support filters: ' + x)
    return 'transaction'
  }

  throw new Error('Only the transaction event is supported in this fork: ' + x)
}
