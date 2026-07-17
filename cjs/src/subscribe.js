const noop = () => { /* noop */ }

module.exports = Subscribe;function Subscribe(postgres, options) {
  const subscribers = new Map()
      , slot = 'postgresjs_' + Math.random().toString(36).slice(2)
      , state = {}
      , hwm = options.subscribe_high_water_mark || 1024
      , lwm = Math.ceil(hwm / 4)

  let connection
    , stream
    , ended = false

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
    onclose: async function() {
      if (ended)
        return
      stream = null
      state.pid = state.secret = undefined
      connected(await init(sql, slot, options.publications))
      subscribers.forEach(event => event.forEach(({ onsubscribe }) => onsubscribe()))
    },
    no_subscribe: true
  })

  const end = sql.end
      , close = sql.close

  sql.end = async() => {
    ended = true
    stream && (await new Promise(r => (stream.once('close', r), stream.end())))
    return end()
  }

  sql.close = async() => {
    stream && (await new Promise(r => (stream.once('close', r), stream.end())))
    return close()
  }

  return subscribe

  async function subscribe(event, fn, onsubscribe = noop, onerror = noop) {
    event = parseEvent(event)

    if (!connection)
      connection = init(sql, slot, options.publications)

    const subscriber = { fn, onsubscribe }
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
      return { unsubscribe, state, sql }
    })
  }

  function connected(x) {
    stream = x.stream
    state.pid = x.state.pid
    state.secret = x.state.secret
  }

  async function init(sql, slot, publications) {
    if (!publications)
      throw new Error('Missing publication names')

    const xs = await sql.unsafe(
      `CREATE_REPLICATION_SLOT ${ slot } TEMPORARY LOGICAL pgoutput NOEXPORT_SNAPSHOT`
    )

    const [x] = xs

    const v2 = parseInt(sql.parameters.server_version) >= 14

    const stream = await sql.unsafe(
      `START_REPLICATION SLOT ${ slot } LOGICAL ${
        x.consistent_point
      } (proto_version '${ v2 ? '2' : '1' }', publication_names '${ publications }'${ v2 ? ', streaming \'on\'' : '' })`
    ).writable()

    const state = {
      lsn: Buffer.concat(x.consistent_point.split('/').map(x => Buffer.from(('00000000' + x).slice(-8), 'hex')))
    }

    const live = new Set()
        , txs = new Map()

    let tx = null
      , begun = null
      , queued = 0
      , paused = false
      , heartbeat = null

    stream.on('data', data)
    stream.on('error', error)
    stream.on('close', teardown)
    stream.on('close', sql.close)

    return { stream, state: xs.state }

    function error(e) {
      console.error('Unexpected error during logical streaming - reconnecting', e) // eslint-disable-line
    }

    function data(x) {
      if (x[0] === 0x77) {
        parse(x.subarray(25), state, sql.options.parsers, handle, options.transform)
      } else if (x[0] === 0x6b && x[17]) {
        state.lsn = x.subarray(1, 9)
        pong()
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
      tx = begun = null
    }

    function streamCommit(b) {
      const t = txs.get(b.xid)
      txs.delete(b.xid)
      t && (t.info.lsn = b.lsn, t.info.date = b.date, t.end())
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
        push: x => t.iterators.forEach(it => it.push(x)),
        end: () => (live.delete(t), t.iterators.forEach(it => it.end())),
        error: e => (live.delete(t), t.iterators.forEach(it => it.error(e)))
      }

      fns.forEach(({ fn }) => {
        const it = Changes()
        t.iterators.push(it)
        try {
          const x = fn(it.changes, info, 'transaction')
          x && typeof x.catch === 'function' && x.catch(error)
        } catch (e) {
          error(e)
        }
      })

      live.add(t)
      return t
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
      heartbeat = null
      paused = false
      queued = 0
      tx = begun = null
      txs.clear()
      const e = new Error('Subscription stream closed')
      live.forEach(t => t.error(e))
      live.clear()
    }

    function pong() {
      const x = Buffer.alloc(34)
      x[0] = 'r'.charCodeAt(0)
      x.fill(state.lsn, 1)
      x.writeBigInt64BE(BigInt(Date.now() - Date.UTC(2000, 0, 1)) * BigInt(1000), 25)
      stream.write(x)
    }
  }

  function call(x, a, b) {
    subscribers.has(x) && subscribers.get(x).forEach(({ fn }) => fn(a, b, x))
  }
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
      handle(null, { command: 'stream_commit', xid: x.readUInt32BE(1), lsn: Lsn(x, 6), date: Time(x.readBigInt64BE(22)) })
    },
    A: x => { // Stream Abort
      handle(null, { command: 'stream_abort', xid: x.readUInt32BE(1), subxid: x.readUInt32BE(5) })
    },
    C: x => { // Commit
      handle(null, { command: 'commit', lsn: Lsn(x, 2), date: Time(x.readBigInt64BE(18)) })
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
