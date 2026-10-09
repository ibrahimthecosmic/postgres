const { exec } = require('./bootstrap.js')

const { t, nt, ot } = require('./test.js') // eslint-disable-line
const net = require('net')
const fs = require('fs')
const crypto = require('crypto')

const postgres = require('../src/index.js')
const { reconnectDelay } = require('../src/subscribe.js')
const delay = ms => new Promise(r => setTimeout(r, ms))

const rel = x => require("path").join(__dirname, x)
const idle_timeout = 1

const login = {
  user: 'postgres_js_test'
}

const login_md5 = {
  user: 'postgres_js_test_md5',
  pass: 'postgres_js_test_md5'
}

const login_scram = {
  user: 'postgres_js_test_scram',
  pass: 'postgres_js_test_scram'
}

const options = {
  db: 'postgres_js_test',
  user: login.user,
  pass: login.pass,
  idle_timeout,
  connect_timeout: 1,
  max: 1
}

const sql = postgres(options)

t('Connects with no options', async() => {
  const sql = postgres({ max: 1 })

  const result = (await sql`select 1 as x`)[0].x
  await sql.end()

  return [1, result]
})

t('Uses default database without slash', async() => {
  const sql = postgres('postgres://localhost')
  return [sql.options.user, sql.options.database]
})

t('Uses default database with slash', async() => {
  const sql = postgres('postgres://localhost/')
  return [sql.options.user, sql.options.database]
})

t('Result is array', async() =>
  [true, Array.isArray(await sql`select 1`)]
)

t('Result has count', async() =>
  [1, (await sql`select 1`).count]
)

t('Result has command', async() =>
  ['SELECT', (await sql`select 1`).command]
)

t('Create table', async() =>
  ['CREATE TABLE', (await sql`create table test(int int)`).command, await sql`drop table test`]
)

t('Drop table', { timeout: 2 }, async() => {
  await sql`create table test(int int)`
  return ['DROP TABLE', (await sql`drop table test`).command]
})

t('null', async() =>
  [null, (await sql`select ${ null } as x`)[0].x]
)

t('Integer', async() =>
  ['1', (await sql`select ${ 1 } as x`)[0].x]
)

t('String', async() =>
  ['hello', (await sql`select ${ 'hello' } as x`)[0].x]
)

t('Boolean false', async() =>
  [false, (await sql`select ${ false } as x`)[0].x]
)

t('Boolean true', async() =>
  [true, (await sql`select ${ true } as x`)[0].x]
)

t('Date', async() => {
  const now = new Date()
  return [0, now - (await sql`select ${ now } as x`)[0].x]
})

t('Json', async() => {
  const x = (await sql`select ${ sql.json({ a: 'hello', b: 42 }) } as x`)[0].x
  return ['hello,42', [x.a, x.b].join()]
})

t('implicit json', async() => {
  const x = (await sql`select ${ { a: 'hello', b: 42 } }::json as x`)[0].x
  return ['hello,42', [x.a, x.b].join()]
})

t('implicit jsonb', async() => {
  const x = (await sql`select ${ { a: 'hello', b: 42 } }::jsonb as x`)[0].x
  return ['hello,42', [x.a, x.b].join()]
})

t('Empty array', async() =>
  [true, Array.isArray((await sql`select ${ sql.array([], 1009) } as x`)[0].x)]
)

t('String array', async() =>
  ['123', (await sql`select ${ '{1,2,3}' }::int[] as x`)[0].x.join('')]
)

t('Array of Integer', async() =>
  ['3', (await sql`select ${ sql.array([1, 2, 3]) } as x`)[0].x[2]]
)

t('Array of String', async() =>
  ['c', (await sql`select ${ sql.array(['a', 'b', 'c']) } as x`)[0].x[2]]
)

t('Array of Date', async() => {
  const now = new Date()
  return [now.getTime(), (await sql`select ${ sql.array([now, now, now]) } as x`)[0].x[2].getTime()]
})

t('Array of Box', async() => [
  '(3,4),(1,2);(6,7),(4,5)',
  (await sql`select ${ '{(1,2),(3,4);(4,5),(6,7)}' }::box[] as x`)[0].x.join(';')
])

t('Nested array n2', async() =>
  ['4', (await sql`select ${ sql.array([[1, 2], [3, 4]]) } as x`)[0].x[1][1]]
)

t('Nested array n3', async() =>
  ['6', (await sql`select ${ sql.array([[[1, 2]], [[3, 4]], [[5, 6]]]) } as x`)[0].x[2][0][1]]
)

t('Escape in arrays', async() =>
  ['Hello "you",c:\\windows', (await sql`select ${ sql.array(['Hello "you"', 'c:\\windows']) } as x`)[0].x.join(',')]
)

t('Escapes', async() => {
  return ['hej"hej', Object.keys((await sql`select 1 as ${ sql('hej"hej') }`)[0])[0]]
})

t('null for int', async() => {
  await sql`create table test (x int)`
  return [1, (await sql`insert into test values(${ null })`).count, await sql`drop table test`]
})

t('Throws on illegal transactions', async() => {
  const sql = postgres({ ...options, max: 2, fetch_types: false })
  const error = await sql`begin`.catch(e => e)
  return [
    error.code,
    'UNSAFE_TRANSACTION'
  ]
})

t('Transaction throws', async() => {
  await sql`create table test (a int)`
  return ['22P02', await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql`insert into test values('hej')`
  }).catch(x => x.code), await sql`drop table test`]
})

t('Transaction rolls back', async() => {
  await sql`create table test (a int)`
  await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql`insert into test values('hej')`
  }).catch(() => { /* ignore */ })
  return [0, (await sql`select a from test`).count, await sql`drop table test`]
})

t('Transaction throws on uncaught savepoint', async() => {
  await sql`create table test (a int)`

  return ['fail', (await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql.savepoint(async sql => {
      await sql`insert into test values(2)`
      throw new Error('fail')
    })
  }).catch((err) => err.message)), await sql`drop table test`]
})

t('Transaction throws on uncaught named savepoint', async() => {
  await sql`create table test (a int)`

  return ['fail', (await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql.savepoit('watpoint', async sql => {
      await sql`insert into test values(2)`
      throw new Error('fail')
    })
  }).catch(() => 'fail')), await sql`drop table test`]
})

t('Transaction succeeds on caught savepoint', async() => {
  await sql`create table test (a int)`
  await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql.savepoint(async sql => {
      await sql`insert into test values(2)`
      throw new Error('please rollback')
    }).catch(() => { /* ignore */ })
    await sql`insert into test values(3)`
  })

  return ['2', (await sql`select count(1) from test`)[0].count, await sql`drop table test`]
})

t('Savepoint returns Result', async() => {
  let result
  await sql.begin(async sql => {
    result = await sql.savepoint(sql =>
      sql`select 1 as x`
    )
  })

  return [1, result[0].x]
})

t('Prepared transaction', async() => {
  await sql`create table test (a int)`

  await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql.prepare('tx1')
  })

  await sql`commit prepared 'tx1'`

  return ['1', (await sql`select count(1) from test`)[0].count, await sql`drop table test`]
})

t('Transaction requests are executed implicitly', async() => {
  const sql = postgres({ debug: true, idle_timeout: 1, fetch_types: false })
  return [
    'testing',
    (await sql.begin(sql => [
      sql`select set_config('postgres_js.test', 'testing', true)`,
      sql`select current_setting('postgres_js.test') as x`
    ]))[1][0].x
  ]
})

t('Uncaught transaction request errors bubbles to transaction', async() => [
  '42703',
  (await sql.begin(sql => [
    sql`select wat`,
    sql`select current_setting('postgres_js.test') as x, ${ 1 } as a`
  ]).catch(e => e.code))
])

t('Fragments in transactions', async() => [
  true,
  (await sql.begin(sql => sql`select true as x where ${ sql`1=1` }`))[0].x
])

t('Transaction rejects with rethrown error', async() => [
  'WAT',
  await sql.begin(async sql => {
    try {
      await sql`select exception`
    } catch (ex) {
      throw new Error('WAT')
    }
  }).catch(e => e.message)
])

t('Parallel transactions', async() => {
  await sql`create table test (a int)`
  return ['11', (await Promise.all([
    sql.begin(sql => sql`select 1`),
    sql.begin(sql => sql`select 1`)
  ])).map(x => x.count).join(''), await sql`drop table test`]
})

t('Many transactions at beginning of connection', async() => {
  const sql = postgres(options)
  const xs = await Promise.all(Array.from({ length: 100 }, () => sql.begin(sql => sql`select 1`)))
  return [100, xs.length]
})

t('Transactions array', async() => {
  await sql`create table test (a int)`

  return ['11', (await sql.begin(sql => [
    sql`select 1`.then(x => x),
    sql`select 1`
  ])).map(x => x.count).join(''), await sql`drop table test`]
})

t('Transaction waits', async() => {
  await sql`create table test (a int)`
  await sql.begin(async sql => {
    await sql`insert into test values(1)`
    await sql.savepoint(async sql => {
      await sql`insert into test values(2)`
      throw new Error('please rollback')
    }).catch(() => { /* ignore */ })
    await sql`insert into test values(3)`
  })

  return ['11', (await Promise.all([
    sql.begin(sql => sql`select 1`),
    sql.begin(sql => sql`select 1`)
  ])).map(x => x.count).join(''), await sql`drop table test`]
})

t('Helpers in Transaction', async() => {
  return ['1', (await sql.begin(async sql =>
    await sql`select ${ sql({ x: 1 }) }`
  ))[0].x]
})

t('Undefined values throws', async() => {
  let error

  await sql`
    select ${ undefined } as x
  `.catch(x => error = x.code)

  return ['UNDEFINED_VALUE', error]
})

t('Transform undefined', async() => {
  const sql = postgres({ ...options, transform: { undefined: null } })
  return [null, (await sql`select ${ undefined } as x`)[0].x]
})

t('Transform undefined in array', async() => {
  const sql = postgres({ ...options, transform: { undefined: null } })
  return [null, (await sql`select * from (values ${ sql([undefined, undefined]) }) as x(x, y)`)[0].y]
})

t('Null sets to null', async() =>
  [null, (await sql`select ${ null } as x`)[0].x]
)

t('Throw syntax error', async() =>
  ['42601', (await sql`wat 1`.catch(x => x)).code]
)

t('Connect using uri', async() =>
  [true, await new Promise((resolve, reject) => {
    const sql = postgres('postgres://' + login.user + ':' + (login.pass || '') + '@localhost:5432/' + options.db, {
      idle_timeout
    })
    sql`select 1`.then(() => resolve(true), reject)
  })]
)

t('Options from uri with special characters in user and pass', async() => {
  const opt = postgres({ user: 'öla', pass: 'pass^word' }).options
  return [[opt.user, opt.pass].toString(), 'öla,pass^word']
})

t('max from the url is a number, and the pool opens that many connections', async() => {
  const env = process.env // eslint-disable-line
  const sql = postgres('postgres://' + (env.PGHOST || 'localhost') + ':' + (env.PGPORT || 5432) + '/' + options.db + '?max=3', {
    user: login.user,
    idle_timeout
  })
  const pids = await Promise.all([1, 2, 3].map(() => sql`select pg_backend_pid() as pid, pg_sleep(0.2)`))
  await sql.end()
  return ['number,3', [typeof sql.options.max, new Set(pids.map(([x]) => x.pid)).size].toString()]
})

t('max from PGMAX is a number', async() => {
  const env = process.env // eslint-disable-line
  const before = env.PGMAX
  env.PGMAX = '4'
  try {
    const { max, ...rest } = options // eslint-disable-line
    const opt = postgres(rest).options
    return ['number,4', [typeof opt.max, opt.max].toString()]
  } finally {
    before === undefined ? delete env.PGMAX : env.PGMAX = before
  }
})

t('Fail with proper error on no host', async() =>
  ['ECONNREFUSED', (await new Promise((resolve, reject) => {
    const sql = postgres('postgres://localhost:33333/' + options.db, {
      idle_timeout
    })
    sql`select 1`.then(reject, resolve)
  })).code]
)

t('Connect using SSL', async() =>
  [true, (await new Promise((resolve, reject) => {
    postgres({
      ssl: { rejectUnauthorized: false },
      idle_timeout
    })`select 1`.then(() => resolve(true), reject)
  }))]
)

t('Connect using SSL require', async() =>
  [true, (await new Promise((resolve, reject) => {
    postgres({
      ssl: 'require',
      idle_timeout
    })`select 1`.then(() => resolve(true), reject)
  }))]
)

t('Connect using SSL direct', async() => {
  const [{ supported }] = await sql`select current_setting('server_version_num')::int >= 180000 as supported`
  return [true, !supported || (await new Promise((resolve, reject) => {
    postgres({
      ssl: 'require',
      sslnegotiation: 'direct',
      idle_timeout
    })`select 1`.then(() => resolve(true), reject)
  }))]
})

t('Connect using SSL prefer', async() => {
  await exec('psql', ['-c', 'alter system set ssl=off'])
  await exec('psql', ['-c', 'select pg_reload_conf()'])

  const sql = postgres({
    ssl: 'prefer',
    idle_timeout
  })

  return [
    1, (await sql`select 1 as x`)[0].x,
    await exec('psql', ['-c', 'alter system set ssl=on']),
    await exec('psql', ['-c', 'select pg_reload_conf()'])
  ]
})

t('Reconnect using SSL', { timeout: 2 }, async() => {
  const sql = postgres({
    ssl: 'require',
    idle_timeout: 0.1
  })

  await sql`select 1`
  await delay(200)

  return [1, (await sql`select 1 as x`)[0].x]
})

t('Proper handling of non object Errors', async() => {
  const sql = postgres({ socket: () => { throw 'wat' } }) // eslint-disable-line

  return [
    'wat', await sql`select 1 as x`.catch(e => e.message)
  ]
})

t('Proper handling of null Errors', async() => {
  const sql = postgres({ socket: () => { throw null } }) // eslint-disable-line

  return [
    'null', await sql`select 1 as x`.catch(e => e.message)
  ]
})

t('Ensure reserve on connection throws proper error', async() => {
  const sql = postgres({ socket: () => { throw 'wat' }, idle_timeout }) // eslint-disable-line

  return [
    'wat', await sql.reserve().catch(e => e)
  ]
})

t('Login without password', async() => {
  return [true, (await postgres({ ...options, ...login })`select true as x`)[0].x]
})

t('Login using MD5', async() => {
  return [true, (await postgres({ ...options, ...login_md5 })`select true as x`)[0].x]
})

t('Login using scram-sha-256', async() => {
  return [true, (await postgres({ ...options, ...login_scram })`select true as x`)[0].x]
})

t('Parallel connections using scram-sha-256', {
  timeout: 2
}, async() => {
  const sql = postgres({ ...options, ...login_scram })
  return [true, (await Promise.all([
    sql`select true as x, pg_sleep(0.01)`,
    sql`select true as x, pg_sleep(0.01)`,
    sql`select true as x, pg_sleep(0.01)`
  ]))[0][0].x]
})

t('Support dynamic password function', async() => {
  return [true, (await postgres({
    ...options,
    ...login_scram,
    pass: () => 'postgres_js_test_scram'
  })`select true as x`)[0].x]
})

t('Support dynamic async password function', async() => {
  return [true, (await postgres({
    ...options,
    ...login_scram,
    pass: () => Promise.resolve('postgres_js_test_scram')
  })`select true as x`)[0].x]
})

t('Point type', async() => {
  const sql = postgres({
    ...options,
    types: {
      point: {
        to: 600,
        from: [600],
        serialize: ([x, y]) => '(' + x + ',' + y + ')',
        parse: (x) => x.slice(1, -1).split(',').map(x => +x)
      }
    }
  })

  await sql`create table test (x point)`
  await sql`insert into test (x) values (${ sql.types.point([10, 20]) })`
  return [20, (await sql`select x from test`)[0].x[1], await sql`drop table test`]
})

t('Point type array', async() => {
  const sql = postgres({
    ...options,
    types: {
      point: {
        to: 600,
        from: [600],
        serialize: ([x, y]) => '(' + x + ',' + y + ')',
        parse: (x) => x.slice(1, -1).split(',').map(x => +x)
      }
    }
  })

  await sql`create table test (x point[])`
  await sql`insert into test (x) values (${ sql.array([sql.types.point([10, 20]), sql.types.point([20, 30])]) })`
  return [30, (await sql`select x from test`)[0].x[1][1], await sql`drop table test`]
})

t('sql file', async() =>
  [1, (await sql.file(rel('select.sql')))[0].x]
)

t('sql file has forEach', async() => {
  let result
  await sql
    .file(rel('select.sql'), { cache: false })
    .forEach(({ x }) => result = x)

  return [1, result]
})

t('sql file throws', async() =>
  ['ENOENT', (await sql.file(rel('selectomondo.sql')).catch(x => x.code))]
)

t('sql file cached', async() => {
  await sql.file(rel('select.sql'))
  await delay(20)

  return [1, (await sql.file(rel('select.sql')))[0].x]
})

t('Parameters in file', async() => {
  const result = await sql.file(
    rel('select-param.sql'),
    ['hello']
  )
  return ['hello', result[0].x]
})

t('Connection ended promise', async() => {
  const sql = postgres(options)

  await sql.end()

  return [undefined, await sql.end()]
})

t('Connection ended timeout', async() => {
  const sql = postgres(options)

  await sql.end({ timeout: 10 })

  return [undefined, await sql.end()]
})

t('Connection ended error', async() => {
  const sql = postgres(options)
  await sql.end()
  return ['CONNECTION_ENDED', (await sql``.catch(x => x.code))]
})

t('Connection end does not cancel query', async() => {
  const sql = postgres(options)

  const promise = sql`select 1 as x`.execute()

  await sql.end()

  return [1, (await promise)[0].x]
})

t('Connection destroyed', async() => {
  const sql = postgres(options)
  process.nextTick(() => sql.end({ timeout: 0 }))
  return ['CONNECTION_DESTROYED', await sql``.catch(x => x.code)]
})

t('Connection destroyed with query before', async() => {
  const sql = postgres(options)
      , error = sql`select pg_sleep(0.2)`.catch(err => err.code)

  sql.end({ timeout: 0 })
  return ['CONNECTION_DESTROYED', await error]
})

t('transform column', async() => {
  const sql = postgres({
    ...options,
    transform: { column: x => x.split('').reverse().join('') }
  })

  await sql`create table test (hello_world int)`
  await sql`insert into test values (1)`
  return ['dlrow_olleh', Object.keys((await sql`select * from test`)[0])[0], await sql`drop table test`]
})

t('column toPascal', async() => {
  const sql = postgres({
    ...options,
    transform: { column: postgres.toPascal }
  })

  await sql`create table test (hello_world int)`
  await sql`insert into test values (1)`
  return ['HelloWorld', Object.keys((await sql`select * from test`)[0])[0], await sql`drop table test`]
})

t('column toCamel', async() => {
  const sql = postgres({
    ...options,
    transform: { column: postgres.toCamel }
  })

  await sql`create table test (hello_world int)`
  await sql`insert into test values (1)`
  return ['helloWorld', Object.keys((await sql`select * from test`)[0])[0], await sql`drop table test`]
})

t('column toKebab', async() => {
  const sql = postgres({
    ...options,
    transform: { column: postgres.toKebab }
  })

  await sql`create table test (hello_world int)`
  await sql`insert into test values (1)`
  return ['hello-world', Object.keys((await sql`select * from test`)[0])[0], await sql`drop table test`]
})

t('Transform nested json in arrays', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })
  return ['aBcD', (await sql`select '[{"a_b":1},{"c_d":2}]'::jsonb as x`)[0].x.map(Object.keys).join('')]
})

t('Transform deeply nested json object in arrays', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })
  return [
    'childObj_deeplyNestedObj_grandchildObj',
    (await sql`
      select '[{"nested_obj": {"child_obj": 2, "deeply_nested_obj": {"grandchild_obj": 3}}}]'::jsonb as x
    `)[0].x.map(x => {
      let result
      for (const key in x)
        result = [...Object.keys(x[key]), ...Object.keys(x[key].deeplyNestedObj)]
      return result
    })[0]
    .join('_')
  ]
})

t('Transform deeply nested json array in arrays', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })
  return [
    'childArray_deeplyNestedArray_grandchildArray',
    (await sql`
      select '[{"nested_array": [{"child_array": 2, "deeply_nested_array": [{"grandchild_array":3}]}]}]'::jsonb AS x
    `)[0].x.map((x) => {
      let result
      for (const key in x)
        result = [...Object.keys(x[key][0]), ...Object.keys(x[key][0].deeplyNestedArray[0])]
      return result
    })[0]
    .join('_')
  ]
})

t('Bypass transform for json primitive', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })

  const x = (
    await sql`select 'null'::json as a, 'false'::json as b, '"a"'::json as c, '1'::json as d`
  )[0]

  return [
    JSON.stringify({ a: null, b: false, c: 'a', d: 1 }),
    JSON.stringify(x)
  ]
})

t('Bypass transform for jsonb primitive', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })

  const x = (
    await sql`select 'null'::jsonb as a, 'false'::jsonb as b, '"a"'::jsonb as c, '1'::jsonb as d`
  )[0]

  return [
    JSON.stringify({ a: null, b: false, c: 'a', d: 1 }),
    JSON.stringify(x)
  ]
})

t('unsafe', async() => {
  await sql`create table test (x int)`
  return [1, (await sql.unsafe('insert into test values ($1) returning *', [1]))[0].x, await sql`drop table test`]
})

t('unsafe simple', async() => {
  return [1, (await sql.unsafe('select 1 as x'))[0].x]
})

t('unsafe simple includes columns', async() => {
  return ['x', (await sql.unsafe('select 1 as x').values()).columns[0].name]
})

t('unsafe describe', async() => {
  const q = 'insert into test values (1)'
  await sql`create table test(a int unique)`
  await sql.unsafe(q).describe()
  const x = await sql.unsafe(q).describe()
  return [
    q,
    x.string,
    await sql`drop table test`
  ]
})

t('simple query using unsafe with multiple statements', async() => {
  return [
    '1,2',
    (await sql.unsafe('select 1 as x;select 2 as x')).map(x => x[0].x).join()
  ]
})

t('simple query using simple() with multiple statements', async() => {
  return [
    '1,2',
    (await sql`select 1 as x;select 2 as x`.simple()).map(x => x[0].x).join()
  ]
})

t('listen and notify', async() => {
  const sql = postgres(options)
  const channel = 'hello'
  const result = await new Promise(async r => {
    await sql.listen(channel, r)
    sql.notify(channel, 'works')
  })

  return [
    'works',
    result,
    sql.end()
  ]
})

t('double listen', async() => {
  const sql = postgres(options)
      , channel = 'hello'

  let count = 0

  await new Promise((resolve, reject) =>
    sql.listen(channel, resolve)
    .then(() => sql.notify(channel, 'world'))
    .catch(reject)
  ).then(() => count++)

  await new Promise((resolve, reject) =>
    sql.listen(channel, resolve)
    .then(() => sql.notify(channel, 'world'))
    .catch(reject)
  ).then(() => count++)

  // for coverage
  sql.listen('weee', () => { /* noop */ }).then(sql.end)

  return [2, count]
})

t('multiple listeners work after a reconnect', async() => {
  const sql = postgres(options)
      , xs = []

  const s1 = await sql.listen('test', x => xs.push('1', x))
  await sql.listen('test', x => xs.push('2', x))
  await sql.notify('test', 'a')
  await delay(50)
  await sql`select pg_terminate_backend(${ s1.state.pid })`
  await delay(200)
  await sql.notify('test', 'b')
  await delay(50)
  sql.end()

  return ['1a2a1b2b', xs.join('')]
})

t('listen and notify with weird name', async() => {
  const sql = postgres(options)
  const channel = 'wat-;.ø.§'
  const result = await new Promise(async r => {
    const { unlisten } = await sql.listen(channel, r)
    sql.notify(channel, 'works')
    await delay(50)
    await unlisten()
  })

  return [
    'works',
    result,
    sql.end()
  ]
})

t('listen and notify with upper case', async() => {
  const sql = postgres(options)
  const channel = 'withUpperChar'
  const result = await new Promise(async r => {
    await sql.listen(channel, r)
    sql.notify(channel, 'works')
  })

  return [
    'works',
    result,
    sql.end()
  ]
})

t('listen reconnects', { timeout: 2 }, async() => {
  const sql = postgres(options)
      , resolvers = {}
      , a = new Promise(r => resolvers.a = r)
      , b = new Promise(r => resolvers.b = r)

  let connects = 0

  const { state: { pid } } = await sql.listen(
    'test',
    x => x in resolvers && resolvers[x](),
    () => connects++
  )
  await sql.notify('test', 'a')
  await a
  await sql`select pg_terminate_backend(${ pid })`
  await delay(100)
  await sql.notify('test', 'b')
  await b
  sql.end()
  return [connects, 2]
})

t('listen result reports correct connection state after reconnection', async() => {
  const sql = postgres(options)
      , xs = []

  const result = await sql.listen('test', x => xs.push(x))
  const initialPid = result.state.pid
  await sql.notify('test', 'a')
  await sql`select pg_terminate_backend(${ initialPid })`
  await delay(50)
  sql.end()

  return [result.state.pid !== initialPid, true]
})

t('unlisten removes subscription', async() => {
  const sql = postgres(options)
      , xs = []

  const { unlisten } = await sql.listen('test', x => xs.push(x))
  await sql.notify('test', 'a')
  await delay(50)
  await unlisten()
  await sql.notify('test', 'b')
  await delay(50)
  sql.end()

  return ['a', xs.join('')]
})

t('listen after unlisten', async() => {
  const sql = postgres(options)
      , xs = []

  const { unlisten } = await sql.listen('test', x => xs.push(x))
  await sql.notify('test', 'a')
  await delay(50)
  await unlisten()
  await sql.notify('test', 'b')
  await delay(50)
  await sql.listen('test', x => xs.push(x))
  await sql.notify('test', 'c')
  await delay(50)
  sql.end()

  return ['ac', xs.join('')]
})

t('multiple listeners and unlisten one', async() => {
  const sql = postgres(options)
      , xs = []

  await sql.listen('test', x => xs.push('1', x))
  const s2 = await sql.listen('test', x => xs.push('2', x))
  await sql.notify('test', 'a')
  await delay(50)
  await s2.unlisten()
  await sql.notify('test', 'b')
  await delay(50)
  sql.end()

  return ['1a2a1b', xs.join('')]
})

t('responds with server parameters (application_name)', async() =>
  ['postgres.js', await new Promise((resolve, reject) => postgres({
    ...options,
    onparameter: (k, v) => k === 'application_name' && resolve(v)
  })`select 1`.catch(reject))]
)

t('has server parameters', async() => {
  return ['postgres.js', (await sql`select 1`.then(() => sql.parameters.application_name))]
})

t('big query body', { timeout: 2 }, async() => {
  await sql`create table test (x int)`
  return [50000, (await sql`insert into test ${
    sql([...Array(50000).keys()].map(x => ({ x })))
  }`).count, await sql`drop table test`]
})

t('Throws if more than 65534 parameters', async() => {
  await sql`create table test (x int)`
  return ['MAX_PARAMETERS_EXCEEDED', (await sql`insert into test ${
    sql([...Array(65535).keys()].map(x => ({ x })))
  }`.catch(e => e.code)), await sql`drop table test`]
})

t('let postgres do implicit cast of unknown types', async() => {
  await sql`create table test (x timestamp with time zone)`
  const [{ x }] = await sql`insert into test values (${ new Date().toISOString() }) returning *`
  return [true, x instanceof Date, await sql`drop table test`]
})

t('only allows one statement', async() =>
  ['42601', await sql`select 1; select 2`.catch(e => e.code)]
)

t('await sql() throws not tagged error', async() => {
  let error
  try {
    await sql('select 1')
  } catch (e) {
    error = e.code
  }
  return ['NOT_TAGGED_CALL', error]
})

t('sql().then throws not tagged error', async() => {
  let error
  try {
    sql('select 1').then(() => { /* noop */ })
  } catch (e) {
    error = e.code
  }
  return ['NOT_TAGGED_CALL', error]
})

t('sql().catch throws not tagged error', async() => {
  let error
  try {
    await sql('select 1')
  } catch (e) {
    error = e.code
  }
  return ['NOT_TAGGED_CALL', error]
})

t('sql().finally throws not tagged error', async() => {
  let error
  try {
    sql('select 1').finally(() => { /* noop */ })
  } catch (e) {
    error = e.code
  }
  return ['NOT_TAGGED_CALL', error]
})

t('little bobby tables', async() => {
  const name = 'Robert\'); DROP TABLE students;--'

  await sql`create table students (name text, age int)`
  await sql`insert into students (name) values (${ name })`

  return [
    name, (await sql`select name from students`)[0].name,
    await sql`drop table students`
  ]
})

t('Connection errors are caught using begin()', {
  timeout: 2
}, async() => {
  let error
  try {
    const sql = postgres({ host: 'localhost', port: 1 })

    await sql.begin(async(sql) => {
      await sql`insert into test (label, value) values (${1}, ${2})`
    })
  } catch (err) {
    error = err
  }

  return [
    true,
    error.code === 'ECONNREFUSED' ||
    error.message === 'Connection refused (os error 61)'
  ]
})

t('dynamic table name', async() => {
  await sql`create table test(a int)`
  return [
    0, (await sql`select * from ${ sql('test') }`).count,
    await sql`drop table test`
  ]
})

t('dynamic schema name', async() => {
  await sql`create table test(a int)`
  return [
    0, (await sql`select * from ${ sql('public') }.test`).count,
    await sql`drop table test`
  ]
})

t('dynamic schema and table name', async() => {
  await sql`create table test(a int)`
  return [
    0, (await sql`select * from ${ sql('public.test') }`).count,
    await sql`drop table test`
  ]
})

t('dynamic column name', async() => {
  return ['!not_valid', Object.keys((await sql`select 1 as ${ sql('!not_valid') }`)[0])[0]]
})

t('dynamic select as', async() => {
  return ['2', (await sql`select ${ sql({ a: 1, b: 2 }) }`)[0].b]
})

t('dynamic select as pluck', async() => {
  return [undefined, (await sql`select ${ sql({ a: 1, b: 2 }, 'a') }`)[0].b]
})

t('dynamic insert', async() => {
  await sql`create table test (a int, b text)`
  const x = { a: 42, b: 'the answer' }

  return ['the answer', (await sql`insert into test ${ sql(x) } returning *`)[0].b, await sql`drop table test`]
})

t('dynamic insert pluck', async() => {
  await sql`create table test (a int, b text)`
  const x = { a: 42, b: 'the answer' }

  return [null, (await sql`insert into test ${ sql(x, 'a') } returning *`)[0].b, await sql`drop table test`]
})

t('dynamic in with empty array', async() => {
  await sql`create table test (a int)`
  await sql`insert into test values (1)`
  return [
    (await sql`select * from test where null in ${ sql([]) }`).count,
    0,
    await sql`drop table test`
  ]
})

t('dynamic in after insert', async() => {
  await sql`create table test (a int, b text)`
  const [{ x }] = await sql`
    with x as (
      insert into test values (1, 'hej')
      returning *
    )
    select 1 in ${ sql([1, 2, 3]) } as x from x
  `
  return [
    true, x,
    await sql`drop table test`
  ]
})

t('array insert', async() => {
  await sql`create table test (a int, b int)`
  return [2, (await sql`insert into test (a, b) values ${ sql([1, 2]) } returning *`)[0].b, await sql`drop table test`]
})

t('where parameters in()', async() => {
  await sql`create table test (x text)`
  await sql`insert into test values ('a')`
  return [
    (await sql`select * from test where x in ${ sql(['a', 'b', 'c']) }`)[0].x,
    'a',
    await sql`drop table test`
  ]
})

t('where parameters in() values before', async() => {
  return [2, (await sql`
    with rows as (
      select * from (values (1), (2), (3), (4)) as x(a)
    )
    select * from rows where a in ${ sql([3, 4]) }
  `).count]
})

t('dynamic multi row insert', async() => {
  await sql`create table test (a int, b text)`
  const x = { a: 42, b: 'the answer' }

  return [
    'the answer',
    (await sql`insert into test ${ sql([x, x]) } returning *`)[1].b, await sql`drop table test`
  ]
})

t('dynamic update', async() => {
  await sql`create table test (a int, b text)`
  await sql`insert into test (a, b) values (17, 'wrong')`

  return [
    'the answer',
    (await sql`update test set ${ sql({ a: 42, b: 'the answer' }) } returning *`)[0].b, await sql`drop table test`
  ]
})

t('dynamic update pluck', async() => {
  await sql`create table test (a int, b text)`
  await sql`insert into test (a, b) values (17, 'wrong')`

  return [
    'wrong',
    (await sql`update test set ${ sql({ a: 42, b: 'the answer' }, 'a') } returning *`)[0].b, await sql`drop table test`
  ]
})

t('dynamic select array', async() => {
  await sql`create table test (a int, b text)`
  await sql`insert into test (a, b) values (42, 'yay')`
  return ['yay', (await sql`select ${ sql(['a', 'b']) } from test`)[0].b, await sql`drop table test`]
})

t('dynamic returning array', async() => {
  await sql`create table test (a int, b text)`
  return [
    'yay',
    (await sql`insert into test (a, b) values (42, 'yay') returning ${ sql(['a', 'b']) }`)[0].b,
    await sql`drop table test`
  ]
})

t('dynamic select args', async() => {
  await sql`create table test (a int, b text)`
  await sql`insert into test (a, b) values (42, 'yay')`
  return ['yay', (await sql`select ${ sql('a', 'b') } from test`)[0].b, await sql`drop table test`]
})

t('dynamic values single row', async() => {
  const [{ b }] = await sql`
    select * from (values ${ sql(['a', 'b', 'c']) }) as x(a, b, c)
  `

  return ['b', b]
})

t('dynamic values multi row', async() => {
  const [, { b }] = await sql`
    select * from (values ${ sql([['a', 'b', 'c'], ['a', 'b', 'c']]) }) as x(a, b, c)
  `

  return ['b', b]
})

t('connection parameters', async() => {
  const sql = postgres({
    ...options,
    connection: {
      'some.var': 'yay'
    }
  })

  return ['yay', (await sql`select current_setting('some.var') as x`)[0].x]
})

t('Multiple queries', async() => {
  const sql = postgres(options)

  return [4, (await Promise.all([
    sql`select 1`,
    sql`select 2`,
    sql`select 3`,
    sql`select 4`
  ])).length]
})

t('Multiple statements', async() =>
  [2, await sql.unsafe(`
    select 1 as x;
    select 2 as a;
  `).then(([, [x]]) => x.a)]
)

t('throws correct error when authentication fails', async() => {
  const sql = postgres({
    ...options,
    ...login_md5,
    pass: 'wrong'
  })
  return ['28P01', await sql`select 1`.catch(e => e.code)]
})

t('notice', async() => {
  let notice
  const log = console.log // eslint-disable-line
  console.log = function(x) { // eslint-disable-line
    notice = x
  }

  const sql = postgres(options)

  await sql`create table if not exists users()`
  await sql`create table if not exists users()`

  console.log = log // eslint-disable-line

  return ['NOTICE', notice.severity]
})

t('notice hook', async() => {
  let notice
  const sql = postgres({
    ...options,
    onnotice: x => notice = x
  })

  await sql`create table if not exists users()`
  await sql`create table if not exists users()`

  return ['NOTICE', notice.severity]
})

t('bytea serializes and parses', async() => {
  const buf = Buffer.from('wat')

  await sql`create table test (x bytea)`
  await sql`insert into test values (${ buf })`

  return [
    buf.toString(),
    (await sql`select x from test`)[0].x.toString(),
    await sql`drop table test`
  ]
})

t('forEach', async() => {
  let result
  await sql`select 1 as x`.forEach(({ x }) => result = x)
  return [1, result]
})

t('forEach returns empty array', async() => {
  return [0, (await sql`select 1 as x`.forEach(() => { /* noop */ })).length]
})

t('Cursor', async() => {
  const order = []
  await sql`select 1 as x union select 2 as x`.cursor(async([x]) => {
    order.push(x.x + 'a')
    await delay(100)
    order.push(x.x + 'b')
  })
  return ['1a1b2a2b', order.join('')]
})

t('Unsafe cursor', async() => {
  const order = []
  await sql.unsafe('select 1 as x union select 2 as x').cursor(async([x]) => {
    order.push(x.x + 'a')
    await delay(100)
    order.push(x.x + 'b')
  })
  return ['1a1b2a2b', order.join('')]
})

t('Cursor custom n', async() => {
  const order = []
  await sql`select * from generate_series(1,20)`.cursor(10, async(x) => {
    order.push(x.length)
  })
  return ['10,10', order.join(',')]
})

t('Cursor custom with rest n', async() => {
  const order = []
  await sql`select * from generate_series(1,20)`.cursor(11, async(x) => {
    order.push(x.length)
  })
  return ['11,9', order.join(',')]
})

t('Cursor custom with less results than batch size', async() => {
  const order = []
  await sql`select * from generate_series(1,20)`.cursor(21, async(x) => {
    order.push(x.length)
  })
  return ['20', order.join(',')]
})

t('Cursor cancel', async() => {
  let result
  await sql`select * from generate_series(1,10) as x`.cursor(async([{ x }]) => {
    result = x
    return sql.CLOSE
  })
  return [1, result]
})

t('Cursor throw', async() => {
  const order = []
  await sql`select 1 as x union select 2 as x`.cursor(async([x]) => {
    order.push(x.x + 'a')
    await delay(100)
    throw new Error('watty')
  }).catch(() => order.push('err'))
  return ['1aerr', order.join('')]
})

t('Cursor error', async() => [
  '42601',
  await sql`wat`.cursor(() => { /* noop */ }).catch((err) => err.code)
])

t('Multiple Cursors', { timeout: 2 }, async() => {
  const result = []
  await sql.begin(async sql => [
    await sql`select 1 as cursor, x from generate_series(1,4) as x`.cursor(async([row]) => {
      result.push(row.x)
      await new Promise(r => setTimeout(r, 20))
    }),
    await sql`select 2 as cursor, x from generate_series(101,104) as x`.cursor(async([row]) => {
      result.push(row.x)
      await new Promise(r => setTimeout(r, 10))
    })
  ])

  return ['1,2,3,4,101,102,103,104', result.join(',')]
})

t('Cursor as async iterator', async() => {
  const order = []
  for await (const [x] of sql`select generate_series(1,2) as x;`.cursor()) {
    order.push(x.x + 'a')
    await delay(10)
    order.push(x.x + 'b')
  }

  return ['1a1b2a2b', order.join('')]
})

t('Cursor as async iterator with break', async() => {
  const order = []
  for await (const xs of sql`select generate_series(1,2) as x;`.cursor()) {
    order.push(xs[0].x + 'a')
    await delay(10)
    order.push(xs[0].x + 'b')
    break
  }

  return ['1a1b', order.join('')]
})

t('Async Iterator Unsafe cursor', async() => {
  const order = []
  for await (const [x] of sql.unsafe('select 1 as x union select 2 as x').cursor()) {
    order.push(x.x + 'a')
    await delay(10)
    order.push(x.x + 'b')
  }
  return ['1a1b2a2b', order.join('')]
})

t('Async Iterator Cursor custom n', async() => {
  const order = []
  for await (const x of sql`select * from generate_series(1,20)`.cursor(10))
    order.push(x.length)

  return ['10,10', order.join(',')]
})

t('Async Iterator Cursor custom with rest n', async() => {
  const order = []
  for await (const x of sql`select * from generate_series(1,20)`.cursor(11))
    order.push(x.length)

  return ['11,9', order.join(',')]
})

t('Async Iterator Cursor custom with less results than batch size', async() => {
  const order = []
  for await (const x of sql`select * from generate_series(1,20)`.cursor(21))
    order.push(x.length)
  return ['20', order.join(',')]
})

t('Async Iterator Cursor throws when its connection dies between batches', async() => {
  let onclose
  const closed = new Promise(r => onclose = r)
  const sql = postgres({ ...options, onclose }) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  let rows = 0

  // The batch has been handed over when the connection dies, so there is no
  // pending promise for the error to reject - the next step has to throw it.
  const error = await (async() => {
    for await (const xs of sql`select * from generate_series(1, 10)`.cursor(2)) {
      rows += xs.length
      rows === 2 && await terminate(pid, closed)
    }
  })().catch(e => e)

  return ['57P01 2', error && error.code + ' ' + rows, await sql.end()]
})

t('Cursor callback that outlives its connection touches nothing after it', async() => {
  let onclose
  const closed = new Promise(r => onclose = r)
  const sql = postgres({ ...options, onclose }) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  let rows = 0
    , reopened
  const next = new Promise(r => reopened = r)

  // The cursor fails while its callback still runs, and the query below
  // reopens the connection before the callback returns. After it returns, the
  // cursor must leave both the dead socket and the reopened one alone.
  const error = await sql`select * from generate_series(1, 10)`.cursor(2, async xs => {
    rows += xs.length
    rows === 2 && (await terminate(pid, closed), reopened(sql`select 1 as x`.execute()))
  }).catch(e => e)

  const [{ x }] = await next
  return ['57P01 2 1', error.code + ' ' + rows + ' ' + x, await sql.end()]
})

t('Transform row', async() => {
  const sql = postgres({
    ...options,
    transform: { row: () => 1 }
  })

  return [1, (await sql`select 'wat'`)[0]]
})

t('Transform row forEach', async() => {
  let result
  const sql = postgres({
    ...options,
    transform: { row: () => 1 }
  })

  await sql`select 1`.forEach(x => result = x)

  return [1, result]
})

t('Transform value', async() => {
  const sql = postgres({
    ...options,
    transform: { value: () => 1 }
  })

  return [1, (await sql`select 'wat' as x`)[0].x]
})

t('Transform columns from', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.fromCamel
  })
  await sql`create table test (a_test int, b_test text)`
  await sql`insert into test ${ sql([{ aTest: 1, bTest: 1 }]) }`
  await sql`update test set ${ sql({ aTest: 2, bTest: 2 }) }`
  return [
    2,
    (await sql`select ${ sql('aTest', 'bTest') } from test`)[0].a_test,
    await sql`drop table test`
  ]
})

t('Transform columns to', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.toCamel
  })
  await sql`create table test (a_test int, b_test text)`
  await sql`insert into test ${ sql([{ a_test: 1, b_test: 1 }]) }`
  await sql`update test set ${ sql({ a_test: 2, b_test: 2 }) }`
  return [
    2,
    (await sql`select a_test, b_test from test`)[0].aTest,
    await sql`drop table test`
  ]
})

t('Transform columns from and to', async() => {
  const sql = postgres({
    ...options,
    transform: postgres.camel
  })
  await sql`create table test (a_test int, b_test text)`
  await sql`insert into test ${ sql([{ aTest: 1, bTest: 1 }]) }`
  await sql`update test set ${ sql({ aTest: 2, bTest: 2 }) }`
  return [
    2,
    (await sql`select ${ sql('aTest', 'bTest') } from test`)[0].aTest,
    await sql`drop table test`
  ]
})

t('Transform columns from and to (legacy)', async() => {
  const sql = postgres({
    ...options,
    transform: {
      column: {
        to: postgres.fromCamel,
        from: postgres.toCamel
      }
    }
  })
  await sql`create table test (a_test int, b_test text)`
  await sql`insert into test ${ sql([{ aTest: 1, bTest: 1 }]) }`
  await sql`update test set ${ sql({ aTest: 2, bTest: 2 }) }`
  return [
    2,
    (await sql`select ${ sql('aTest', 'bTest') } from test`)[0].aTest,
    await sql`drop table test`
  ]
})

t('Unix socket', async() => {
  const sql = postgres({
    ...options,
    host: process.env.PGSOCKET || '/tmp' // eslint-disable-line
  })

  return [1, (await sql`select 1 as x`)[0].x]
})

t('Big result', async() => {
  return [100000, (await sql`select * from generate_series(1, 100000)`).count]
})

t('Debug', async() => {
  let result
  const sql = postgres({
    ...options,
    debug: (connection_id, str) => result = str
  })

  await sql`select 1`

  return ['select 1', result]
})

t('bigint is returned as String', async() => [
  'string',
  typeof (await sql`select 9223372036854777 as x`)[0].x
])

t('int is returned as Number', async() => [
  'number',
  typeof (await sql`select 123 as x`)[0].x
])

t('numeric is returned as string', async() => [
  'string',
  typeof (await sql`select 1.2 as x`)[0].x
])

t('Async stack trace', async() => {
  const sql = postgres({ ...options, debug: false })
  return [
    parseInt(new Error().stack.split('\n')[1].match(':([0-9]+):')[1]) + 1,
    parseInt(await sql`error`.catch(x => x.stack.split('\n').pop().match(':([0-9]+):')[1]))
  ]
})

t('Debug has long async stack trace', async() => {
  const sql = postgres({ ...options, debug: true })

  return [
    'watyo',
    await yo().catch(x => x.stack.match(/wat|yo/g).join(''))
  ]

  function yo() {
    return wat()
  }

  function wat() {
    return sql`error`
  }
})

t('Error contains query string', async() => [
  'selec 1',
  (await sql`selec 1`.catch(err => err.query))
])

t('Error contains query serialized parameters', async() => [
  1,
  (await sql`selec ${ 1 }`.catch(err => err.parameters[0]))
])

t('Error contains query raw parameters', async() => [
  1,
  (await sql`selec ${ 1 }`.catch(err => err.args[0]))
])

t('Query and parameters on errorare not enumerable if debug is not set', async() => {
  const sql = postgres({ ...options, debug: false })

  return [
    false,
    (await sql`selec ${ 1 }`.catch(err => err.propertyIsEnumerable('parameters') || err.propertyIsEnumerable('query')))
  ]
})

t('Query and parameters are enumerable if debug is set', async() => {
  const sql = postgres({ ...options, debug: true })

  return [
    true,
    (await sql`selec ${ 1 }`.catch(err => err.propertyIsEnumerable('parameters') && err.propertyIsEnumerable('query')))
  ]
})

t('connect_timeout', { timeout: 20 }, async() => {
  const connect_timeout = 0.2
  const server = net.createServer()
  server.listen()
  const sql = postgres({ port: server.address().port, host: '127.0.0.1', connect_timeout })
  const start = Date.now()
  let end
  await sql`select 1`.catch((e) => {
    if (e.code !== 'CONNECT_TIMEOUT')
      throw e
    end = Date.now()
  })
  server.close()
  return [connect_timeout, Math.floor((end - start) / 100) / 10]
})

t('connect_timeout throws proper error', async() => [
  'CONNECT_TIMEOUT',
  await postgres({
    ...options,
    ...login_scram,
    connect_timeout: 0.001
  })`select 1`.catch(e => e.code)
])

t('connect_timeout error message includes host:port', { timeout: 20 }, async() => {
  const connect_timeout = 0.2
  const server = net.createServer()
  server.listen()
  const sql = postgres({ port: server.address().port, host: '127.0.0.1', connect_timeout })
  const port = server.address().port
  let err
  await sql`select 1`.catch((e) => {
    if (e.code !== 'CONNECT_TIMEOUT')
      throw e
    err = e.message
  })
  server.close()
  return [['write CONNECT_TIMEOUT 127.0.0.1:', port].join(''), err]
})

t('requests works after single connect_timeout', async() => {
  let first = true

  const sql = postgres({
    ...options,
    ...login_scram,
    connect_timeout: { valueOf() { return first ? (first = false, 0.0001) : 1 } }
  })

  return [
    'CONNECT_TIMEOUT,,1',
    [
      await sql`select 1 as x`.then(() => 'success', x => x.code),
      await delay(10),
      (await sql`select 1 as x`)[0].x
    ].join(',')
  ]
})

t('Postgres errors are of type PostgresError', async() =>
  [true, (await sql`bad keyword`.catch(e => e)) instanceof sql.PostgresError]
)

t('Result has columns spec', async() =>
  ['x', (await sql`select 1 as x`).columns[0].name]
)

t('forEach has result as second argument', async() => {
  let x
  await sql`select 1 as x`.forEach((_, result) => x = result)
  return ['x', x.columns[0].name]
})

t('Result as arrays', async() => {
  const sql = postgres({
    ...options,
    transform: {
      row: x => Object.values(x)
    }
  })

  return ['1,2', (await sql`select 1 as a, 2 as b`)[0].join(',')]
})

t('Insert empty array', async() => {
  await sql`create table tester (ints int[])`
  return [
    Array.isArray((await sql`insert into tester (ints) values (${ sql.array([]) }) returning *`)[0].ints),
    true,
    await sql`drop table tester`
  ]
})

t('Insert array in sql()', async() => {
  await sql`create table tester (ints int[])`
  return [
    Array.isArray((await sql`insert into tester ${ sql({ ints: sql.array([]) }) } returning *`)[0].ints),
    true,
    await sql`drop table tester`
  ]
})

t('Automatically creates prepared statements', async() => {
  const sql = postgres(options)
  const result = await sql`select * from pg_prepared_statements`
  return [true, result.some(x => x.name = result.statement.name)]
})

t('no_prepare: true disables prepared statements (deprecated)', async() => {
  const sql = postgres({ ...options, no_prepare: true })
  const result = await sql`select * from pg_prepared_statements`
  return [false, result.some(x => x.name = result.statement.name)]
})

t('prepare: false disables prepared statements', async() => {
  const sql = postgres({ ...options, prepare: false })
  const result = await sql`select * from pg_prepared_statements`
  return [false, result.some(x => x.name = result.statement.name)]
})

t('prepare: true enables prepared statements', async() => {
  const sql = postgres({ ...options, prepare: true })
  const result = await sql`select * from pg_prepared_statements`
  return [true, result.some(x => x.name = result.statement.name)]
})

t('prepares unsafe query when "prepare" option is true', async() => {
  const sql = postgres({ ...options, prepare: true })
  const result = await sql.unsafe('select * from pg_prepared_statements where name <> $1', ['bla'], { prepare: true })
  return [true, result.some(x => x.name = result.statement.name)]
})

t('does not prepare unsafe query by default', async() => {
  const sql = postgres({ ...options, prepare: true })
  const result = await sql.unsafe('select * from pg_prepared_statements where name <> $1', ['bla'])
  return [false, result.some(x => x.name = result.statement.name)]
})

t('Recreate prepared statements on transformAssignedExpr error', { timeout: 1 }, async() => {
  const insert = () => sql`insert into test (name) values (${ '1' }) returning name`
  await sql`create table test (name text)`
  await insert()
  await sql`alter table test alter column name type int using name::integer`
  return [
    1,
    (await insert())[0].name,
    await sql`drop table test`
  ]
})

t('Throws correct error when retrying in transactions', async() => {
  await sql`create table test(x int)`
  const error = await sql.begin(sql => sql`insert into test (x) values (${ false })`).catch(e => e)
  return [
    error.code,
    '42804',
    sql`drop table test`
  ]
})

t('Recreate prepared statements on RevalidateCachedQuery error', async() => {
  const select = () => sql`select name from test`
  await sql`create table test (name text)`
  await sql`insert into test values ('1')`
  await select()
  await sql`alter table test alter column name type int using name::integer`
  return [
    1,
    (await select())[0].name,
    await sql`drop table test`
  ]
})

t('A failed retry reports its own error, the trigger as cause', async() => {
  await sql`create table test (id int, v varchar(10))`
  const insert = (id, v) => sql`insert into test (id, v) values (${ id }, ${ v }) returning *`
  await insert(1, 'ok')
  // `returning *` changes shape, so the cached plan is invalidated and the
  // next execution of the same statement retries.
  await sql`alter table test add column w int`
  // The re-prepared run fails on the value. That is the caller's error; the
  // stale-plan error that triggered the retry is an internal detail.
  const error = await insert(2, 'x'.repeat(50)).catch(e => e)
  return [
    '22001 0A000',
    error.code + ' ' + (error.cause && error.cause.code),
    await sql`drop table test`
  ]
})

t('Properly throws routine error on not prepared statements', async() => {
  await sql`create table x (x text[])`
  const { routine } = await sql.unsafe(`
    insert into x(x) values (('a', 'b'))
  `).catch(e => e)

  return ['transformAssignedExpr', routine, await sql`drop table x`]
})

t('Properly throws routine error on not prepared statements in transaction', async() => {
  const { routine } = await sql.begin(sql => [
    sql`create table x (x text[])`,
    sql`insert into x(x) values (('a', 'b'))`
  ]).catch(e => e)

  return ['transformAssignedExpr', routine]
})

t('Properly throws routine error on not prepared statements using file', async() => {
  const { routine } = await sql.unsafe(`
    create table x (x text[]);
    insert into x(x) values (('a', 'b'));
  `, { prepare: true }).catch(e => e)

  return ['transformAssignedExpr', routine]
})

t('Catches connection config errors', async() => {
  const sql = postgres({ ...options, user: { toString: () => { throw new Error('wat') } }, database: 'prut' })

  return [
    'wat',
    await sql`select 1`.catch((e) => e.message)
  ]
})

t('Catches connection config errors with end', async() => {
  const sql = postgres({ ...options, user: { toString: () => { throw new Error('wat') } }, database: 'prut' })

  return [
    'wat',
    await sql`select 1`.catch((e) => e.message),
    await sql.end()
  ]
})

t('End settles while a connection is still failing to connect', { timeout: 5 }, async() => {
  // Two concurrent queries open two connections; the first refusal rejects
  // Promise.all and end() runs while the second is still connecting. Its
  // socket then dies with no work left, and end() must settle on that.
  const sql = postgres({ ...options, port: 1, connect_timeout: 2, max: 2 })

  return [
    'ECONNREFUSED',
    await Promise.all([sql`select 1`, sql`select 2`]).catch((e) => e.code),
    await sql.end()
  ]
})

t('Catches query format errors', async() => [
  'wat',
  await sql.unsafe({ toString: () => { throw new Error('wat') } }).catch((e) => e.message)
])

t('Multiple hosts', {
  timeout: 1
}, async() => {
  const s1 = postgres({ idle_timeout })
      , s2 = postgres({ idle_timeout, port: 5433 })
      , sql = postgres('postgres://localhost:5432,localhost:5433', { idle_timeout, max: 1 })
      , result = []

  const id1 = (await s1`select system_identifier as x from pg_control_system()`)[0].x
  const id2 = (await s2`select system_identifier as x from pg_control_system()`)[0].x

  const x1 = await sql`select 1`
  result.push((await sql`select system_identifier as x from pg_control_system()`)[0].x)
  await s1`select pg_terminate_backend(${ x1.state.pid }::int)`
  await delay(50)

  const x2 = await sql`select 1`
  result.push((await sql`select system_identifier as x from pg_control_system()`)[0].x)
  await s2`select pg_terminate_backend(${ x2.state.pid }::int)`
  await delay(50)

  result.push((await sql`select system_identifier as x from pg_control_system()`)[0].x)

  return [[id1, id2, id1].join(','), result.join(',')]
})

t('Escaping supports schemas and tables', async() => {
  await sql`create schema a`
  await sql`create table a.b (c int)`
  await sql`insert into a.b (c) values (1)`
  return [
    1,
    (await sql`select ${ sql('a.b.c') } from a.b`)[0].c,
    await sql`drop table a.b`,
    await sql`drop schema a`
  ]
})

t('Raw method returns rows as arrays', async() => {
  const [x] = await sql`select 1`.raw()
  return [
    Array.isArray(x),
    true
  ]
})

t('Raw method returns values unparsed as Buffer', async() => {
  const [[x]] = await sql`select 1`.raw()
  return [
    x instanceof Uint8Array,
    true
  ]
})

t('Array returns rows as arrays of columns', async() => {
  return [(await sql`select 1`.values())[0][0], 1]
})

t('Copy read', async() => {
  const result = []

  await sql`create table test (x int)`
  await sql`insert into test select * from generate_series(1,10)`
  const readable = await sql`copy test to stdout`.readable()
  readable.on('data', x => result.push(x))
  await new Promise(r => readable.on('end', r))

  return [
    result.length,
    10,
    await sql`drop table test`
  ]
})

t('Copy write', { timeout: 2 }, async() => {
  await sql`create table test (x int)`
  const writable = await sql`copy test from stdin`.writable()

  writable.write('1\n')
  writable.write('1\n')
  writable.end()

  await new Promise(r => writable.on('finish', r))

  return [
    (await sql`select 1 from test`).length,
    2,
    await sql`drop table test`
  ]
})

t('Copy write as first', async() => {
  await sql`create table test (x int)`
  const first = postgres(options)
  const writable = await first`COPY test FROM STDIN WITH(FORMAT csv, HEADER false, DELIMITER ',')`.writable()
  writable.write('1\n')
  writable.write('1\n')
  writable.end()

  await new Promise(r => writable.on('finish', r))

  return [
    (await sql`select 1 from test`).length,
    2,
    await sql`drop table test`
  ]
})

t('Copy from file', async() => {
  await sql`create table test (x int, y int, z int)`
  await new Promise(async r => fs
    .createReadStream(rel('copy.csv'))
    .pipe(await sql`copy test from stdin`.writable())
    .on('finish', r)
  )

  return [
    JSON.stringify(await sql`select * from test`),
    '[{"x":1,"y":2,"z":3},{"x":4,"y":5,"z":6}]',
    await sql`drop table test`
  ]
})

t('Copy from works in transaction', async() => {
  await sql`create table test(x int)`
  const xs = await sql.begin(async sql => {
    (await sql`copy test from stdin`.writable()).end('1\n2')
    await delay(20)
    return sql`select 1 from test`
  })

  return [
    xs.length,
    2,
    await sql`drop table test`
  ]
})

t('Copy from abort', async() => {
  const sql = postgres(options)
  const readable = fs.createReadStream(rel('copy.csv'))

  await sql`create table test (x int, y int, z int)`
  await sql`TRUNCATE TABLE test`

  const writable = await sql`COPY test FROM STDIN`.writable()

  let aborted

  readable
    .pipe(writable)
    .on('error', (err) => aborted = err)

  writable.destroy(new Error('abort'))
  await sql.end()

  return [
    'abort',
    aborted.message,
    await postgres(options)`drop table test`
  ]
})

t('multiple queries before connect', async() => {
  const sql = postgres({ ...options, max: 2 })
  const xs = await Promise.all([
    sql`select 1 as x`,
    sql`select 2 as x`,
    sql`select 3 as x`,
    sql`select 4 as x`
  ])

  return [
    '1,2,3,4',
    xs.map(x => x[0].x).join()
  ]
})

t('subscribe only supports transaction events', async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  const message = x => sql.subscribe(x, () => { /* noop */ }).then(() => 'subscribed', e => e.message)

  return [
    [
      'Only the transaction event is supported in this fork: *',
      'Only the transaction event is supported in this fork: insert:test',
      'The transaction event does not support filters: transaction:foo'
    ].join('|'),
    [await message('*'), await message('insert:test'), await message('transaction:foo')].join('|'),
    await sql.end()
  ]
})

t('subscribe with transform', { timeout: 2 }, async() => {
  const sql = postgres({
    transform: {
      column: {
        from: postgres.toCamel,
        to: postgres.fromCamel
      }
    },
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    for await (const c of changes)
      result.push(c.command, c.row.nameInCamel || c.row.id, c.old && c.old.nameInCamel)
  })

  await sql`
    create table test (
      id serial primary key,
      name_in_camel text
    )
  `

  await sql`insert into test (name_in_camel) values ('Murray')`
  await sql`update test set name_in_camel = 'Rothbard'`
  await sql`delete from test`
  await sql`alter table test replica identity full`
  await sql`insert into test (name_in_camel) values ('Murray')`
  await sql`update test set name_in_camel = 'Rothbard'`
  await sql`delete from test`
  await delay(200)
  await unsubscribe()
  return [
    'insert,Murray,,update,Rothbard,,delete,1,,insert,Murray,,update,Rothbard,Murray,delete,Rothbard,',
    result.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe reconnects and calls onsubscribe', { timeout: 4 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []
  let onsubscribes = 0

  const { unsubscribe, sql: subscribeSql } = await sql.subscribe(
    'transaction',
    async changes => {
      for await (const c of changes)
        result.push(c.command, c.row.name || c.row.id)
    },
    () => onsubscribes++
  )

  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  await sql`insert into test (name) values ('Murray')`
  await delay(200)
  await subscribeSql.close()
  await delay(500)
  await sql`delete from test`
  await delay(100)
  await unsubscribe()
  return [
    '2insert,Murray,delete,1',
    onsubscribes + result.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe survives walsender termination', { timeout: 10 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []
  let onsubscribes = 0

  const { unsubscribe } = await sql.subscribe(
    'transaction',
    async changes => {
      try {
        for await (const c of changes)
          result.push(c.command)
      } catch (e) {
        // connection loss rejects live iterators — expected here
      }
    },
    () => onsubscribes++
  )

  await sql`create table test (id serial primary key)`
  await sql`insert into test default values`
  await delay(200)
  // Kill the replication connection server-side: the reconnect must be
  // serialized and guarded — no unhandled rejection, stream resumes.
  await sql`select pg_terminate_backend(pid) from pg_stat_activity where backend_type = 'walsender'`
  await delay(1000)
  await sql`insert into test default values`
  await delay(300)
  await unsubscribe()
  return [
    '2insert,insert',
    onsubscribes + result.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe reports each stream loss to onerror and prints nothing', { timeout: 10 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')

  const errors = []
      , printed = []
      , consoleError = console.error // eslint-disable-line
  let onsubscribes = 0

  console.error = (...xs) => printed.push(xs) // eslint-disable-line
  try {
    const { unsubscribe } = await sql.subscribe(
      'transaction',
      () => { /* noop */ },
      () => onsubscribes++,
      e => errors.push(e)
    )

    // Two outages: the second stream is a reconnect's, and has to report to
    // onerror just like the first.
    for (let i = 1; i <= 2; i++) {
      await sql`select pg_terminate_backend(pid) from pg_stat_activity where backend_type = 'walsender'`
      while (onsubscribes <= i)
        await delay(20)
    }
    await unsubscribe()
  } finally {
    console.error = consoleError // eslint-disable-line
  }

  return [
    '2 0',
    errors.length + ' ' + printed.length,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe reports a rejecting transaction handler to its onerror', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')

  const errors = []
  const { unsubscribe } = await sql.subscribe(
    'transaction',
    async() => { throw new Error('handler failed') },
    () => { /* noop */ },
    e => errors.push(e.message)
  )

  await sql`create table test (id serial primary key)`
  await sql`insert into test default values`
  await delay(200)
  await unsubscribe()
  return [
    'handler failed',
    errors.join(),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []
  let callbacks = 0
    , info

  const { unsubscribe } = await sql.subscribe('transaction', async(changes, x) => {
    callbacks++
    for await (const c of changes)
      result.push(c.command, c.row && c.row.name)
    info = x
  })

  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  await sql.begin(async sql => {
    await sql`insert into test (name) values ('Murray')`
    await sql`update test set name = 'Rothbard'`
    await sql`delete from test`
  })
  await delay(200)
  await unsubscribe()
  return [
    '1,insert,Murray,update,Rothbard,delete,,false,string,true',
    callbacks + ',' + result.join(',') + ',' + info.streaming + ',' + typeof info.lsn + ',' + (info.date instanceof Date),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction streaming', { timeout: 10 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  const [{ v }] = await sql`select current_setting('server_version_num')::int as v`
  if (v < 140000)
    return ['skip', 'skip', await sql.end()]

  await sql`alter system set logical_decoding_work_mem = '64kB'`
  await sql`select pg_reload_conf()`
  await delay(100)
  await sql.unsafe('create publication alltables for all tables')
  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  let count = 0
    , streaming = false
    , lsn

  const { unsubscribe } = await sql.subscribe('transaction', async(changes, info) => {
    for await (const c of changes)
      c.command === 'insert' && count++
    streaming = info.streaming
    lsn = info.lsn
  })

  await sql`insert into test (name) select repeat('x', 1000) from generate_series(1, 500)`
  await delay(500)
  await unsubscribe()
  await sql`alter system reset logical_decoding_work_mem`
  await sql`select pg_reload_conf()`
  return [
    'true 500 string',
    streaming + ' ' + count + ' ' + typeof lsn,
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction streamed abort rejects iterator', { timeout: 10 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  const [{ v }] = await sql`select current_setting('server_version_num')::int as v`
  if (v < 140000)
    return ['skip', 'skip', await sql.end()]

  await sql`alter system set logical_decoding_work_mem = '64kB'`
  await sql`select pg_reload_conf()`
  await delay(100)
  await sql.unsafe('create publication alltables for all tables')
  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  let error
    , got = 0

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    try {
      for await (const c of changes)
        c && got++
    } catch (e) {
      error = e
    }
  })

  await sql.begin(async sql => {
    await sql`insert into test (name) select repeat('x', 1000) from generate_series(1, 500)`
    await delay(500)
    throw new Error('rollback')
  }).catch(() => { /* expected */ })
  await delay(500)
  await unsubscribe()
  await sql`alter system reset logical_decoding_work_mem`
  await sql`select pg_reload_conf()`
  return [
    'true',
    '' + /aborted/.test(error && error.message),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction truncate', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    for await (const c of changes) {
      c.command === 'truncate'
        ? result.push(c.command, c.relations.map(r => r.table).join('+'), c.cascade, c.restartIdentity)
        : result.push(c.command, c.row.name)
    }
  })

  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  await sql.begin(async sql => {
    await sql`insert into test (name) values ('Murray')`
    await sql`truncate test restart identity`
  })
  await delay(200)
  await unsubscribe()
  return [
    'insert,Murray,truncate,test,false,true',
    result.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction streamed truncate', { timeout: 10 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  const [{ v }] = await sql`select current_setting('server_version_num')::int as v`
  if (v < 140000)
    return ['skip', 'skip', await sql.end()]

  await sql`alter system set logical_decoding_work_mem = '64kB'`
  await sql`select pg_reload_conf()`
  await delay(100)
  await sql.unsafe('create publication alltables for all tables')
  await sql`
    create table test (
      id serial primary key,
      name text
    )
  `

  let inserts = 0
    , truncated = ''
    , streaming = false

  const { unsubscribe } = await sql.subscribe('transaction', async(changes, info) => {
    for await (const c of changes) {
      c.command === 'insert' && inserts++
      c.command === 'truncate' && (truncated = c.relations.map(r => r.table) + ' ' + c.cascade + ' ' + c.restartIdentity)
    }
    streaming = info.streaming
  })

  await sql.begin(async sql => {
    await sql`insert into test (name) select repeat('x', 1000) from generate_series(1, 500)`
    await sql`truncate test`
  })
  await delay(500)
  await unsubscribe()
  await sql`alter system reset logical_decoding_work_mem`
  await sql`select pg_reload_conf()`
  return [
    'true 500 test false false',
    streaming + ' ' + inserts + ' ' + truncated,
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe transaction subscribe_tables', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    subscribe_tables: ['public.watched']
  })

  await sql.unsafe('create publication alltables for all tables')

  const events = []

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    const changed = []
    events.push(changed)
    for await (const c of changes) {
      c.command === 'truncate'
        ? changed.push(c.command, c.relations.map(r => r.table).join('+'))
        : changed.push(c.command, c.relation.table, c.row.name)
    }
  })

  await sql`create table watched (id serial primary key, name text)`
  await sql`create table ignored (id serial primary key, name text)`
  // PG < 15 sends begin/commit for the DDL above too - start counting after it
  await delay(200)
  events.length = 0

  await sql.begin(async sql => {
    await sql`insert into ignored (name) values ('Keynes')`
  })

  await sql.begin(async sql => {
    await sql`insert into watched (name) values ('Murray')`
    await sql`insert into ignored (name) values ('Marx')`
    await sql`truncate watched, ignored`
  })

  await delay(200)
  await unsubscribe()
  return [
    '|insert,watched,Murray,truncate,watched',
    events.map(x => x.join(',')).join('|'),
    await sql`drop table watched`,
    await sql`drop table ignored`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe_tables takes a predicate', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    subscribe_tables: (schema, table) => schema === 'public' && table.startsWith('watched')
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    for await (const c of changes)
      result.push(c.relation.table, c.row.name)
  })

  await sql`create table watched_one (id serial primary key, name text)`
  await sql`create table ignored (id serial primary key, name text)`
  await delay(200)

  await sql.begin(async sql => {
    await sql`insert into ignored (name) values ('Keynes')`
    await sql`insert into watched_one (name) values ('Murray')`
  })

  await delay(200)
  await unsubscribe()
  return [
    'watched_one,Murray',
    result.join(','),
    await sql`drop table watched_one`,
    await sql`drop table ignored`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe_tables predicate follows the set at runtime', { timeout: 5 }, async() => {
  const watched = new Set(['public.watched_one'])
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    subscribe_tables: (schema, table) => watched.has(schema + '.' + table)
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    for await (const c of changes)
      result.push(c.relation.table + ':' + c.row.name)
  })

  await sql`create table watched_one (id serial primary key, name text)`
  await sql`create table later (id serial primary key, name text)`
  await delay(200)

  // `later` is announced (R) while it fails the test, and pgoutput announces
  // it only once per session - so a remembered verdict would make the widening
  // below silently permanent.
  await sql`insert into later (name) values ('Keynes')`
  await delay(200)

  watched.add('public.later')
  await sql`insert into later (name) values ('Murray')`
  await delay(200)

  watched.delete('public.later')
  await sql`insert into later (name) values ('Marx')`
  await sql`insert into watched_one (name) values ('Menger')`
  await delay(200)

  await unsubscribe()
  return [
    'later:Murray,watched_one:Menger',
    result.join(','),
    await sql`drop table watched_one`,
    await sql`drop table later`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe_tables requires schema qualified names', async() => {
  let error
  try {
    postgres({ ...options, subscribe_tables: ['users'] })
  } catch (e) {
    error = e
  }

  return [true, /schema qualified/.test(error && error.message)]
})

t('subscribe transaction subscribe_raw', { timeout: 5 }, async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    subscribe_raw: true
  })

  await sql.unsafe('create publication alltables for all tables')

  const result = []
  let columns

  const { unsubscribe } = await sql.subscribe('transaction', async changes => {
    for await (const c of changes) {
      columns = c.relation.columns
      result.push(typeof c.row.id, c.row.id, typeof c.row.n, c.row.n, c.row.name === null)
    }
  })

  await sql`create table test (id serial primary key, n numeric, name text)`
  await sql`insert into test (n, name) values (1.5, null)`
  await delay(200)
  await unsubscribe()

  return [
    'string,1,string,1.5,true|23,undefined',
    result.join(',') + '|' + columns[0].type + ',' + typeof columns[0].parser,
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe durable slot resumes from the confirmed lsn', { timeout: 20 }, async() => {
  const slot = 'postgresjs_test_durable'
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')
  await sql`select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = ${ slot }`
  await sql`create table test (id serial primary key, name text)`

  const seen = []
  const subscriber = () => postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    slot
  })

  const listen = x => x.subscribe('transaction', async changes => {
    for await (const c of changes)
      c.command === 'insert' && seen.push(c.row.name)
  })

  const first = subscriber()
  const a = await listen(first)
  await sql`insert into test (name) values ('a')`
  await delay(300)

  // Ending the subscription must leave the slot behind - that is the point
  await a.sql.end()
  await first.end()
  await delay(300)

  // Changes committed while nobody is listening are retained by the slot
  await sql`insert into test (name) values ('b')`
  await sql`insert into test (name) values ('c')`

  const second = subscriber()
  const b = await listen(second)
  await delay(500)

  const [{ kept }] = await sql`select count(*)::int as kept from pg_replication_slots where slot_name = ${ slot }`
  await b.drop()
  const [{ kept: dropped }] = await sql`select count(*)::int as kept from pg_replication_slots where slot_name = ${ slot }`
  await second.end()

  return [
    'a,b,c 1 0',
    seen.join(',') + ' ' + kept + ' ' + dropped,
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe durable slot confirms only what the consumer acked', { timeout: 20 }, async() => {
  const slot = 'postgresjs_test_ack'
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')
  await sql`select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = ${ slot }`
  await sql`create table test (id serial primary key, name text)`

  const subscriber = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  let release
    , info

  const gate = new Promise(r => release = r)

  const handle = await subscriber.subscribe('transaction', async(changes, x) => {
    for await (const c of changes) // eslint-disable-line
      ;
    info = x
    await gate
  }, undefined, undefined, { slot })

  await sql`insert into test (name) values ('a')`
  await delay(400)

  const [{ pending }] = await sql`
    select confirmed_flush_lsn < ${ info.lsn }::pg_lsn as pending
    from pg_replication_slots where slot_name = ${ slot }
  `

  release()
  await delay(500)

  const [{ advanced }] = await sql`
    select confirmed_flush_lsn > ${ info.lsn }::pg_lsn as advanced
    from pg_replication_slots where slot_name = ${ slot }
  `

  await handle.drop()
  await subscriber.end()

  return [
    'true true ' + slot,
    pending + ' ' + advanced + ' ' + handle.slot,
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe durable slot replays transactions the consumer never acked', { timeout: 20 }, async() => {
  const slot = 'postgresjs_test_replay'
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')
  await sql`select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = ${ slot }`
  await sql`create table test (id serial primary key, name text)`

  const subscriber = () => postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    slot
  })

  const seen = []
      , replayed = []

  const gate = new Promise(() => { /* never settles */ })

  const first = subscriber()
  const a = await first.subscribe('transaction', async changes => {
    for await (const c of changes)
      c.command === 'insert' && seen.push(c.row.name)
    // Never settles for 'b', so that transaction is handled but never acked
    seen[seen.length - 1] === 'b' && await gate
  })

  await sql`insert into test (name) values ('a')`
  await delay(300)
  await sql`insert into test (name) values ('b')`
  await delay(300)

  await a.sql.end()
  await first.end()
  await delay(300)

  const second = subscriber()
  const b = await second.subscribe('transaction', async changes => {
    for await (const c of changes)
      c.command === 'insert' && replayed.push(c.row.name)
  })
  await delay(700)

  await b.drop()
  await second.end()

  return [
    'a,b b',
    seen.join(',') + ' ' + replayed.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe rejects invalid slot names', async() => {
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables'
  })

  const message = x => sql
    .subscribe('transaction', () => { /* noop */ }, undefined, undefined, { slot: x })
    .then(() => 'subscribed', e => e.message)

  return [
    [
      'Invalid replication slot name: Nope - use lowercase letters, digits and underscores (max 63)',
      'Invalid replication slot name: a"b - use lowercase letters, digits and underscores (max 63)'
    ].join('|'),
    [await message('Nope'), await message('a"b')].join('|'),
    await sql.end()
  ]
})

t('subscribe durable slot reports resumed to onsubscribe', { timeout: 20 }, async() => {
  const slot = 'postgresjs_test_resumed'
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')
  await sql`select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = ${ slot }`
  await sql`create table test (id serial primary key, name text)`

  const subscriber = x => postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    ...x
  })

  const infos = []
      , seen = []

  const listen = (x, tag) => x.subscribe(
    'transaction',
    async changes => {
      try {
        for await (const c of changes)
          c.command === 'insert' && seen.push(tag + c.row.name)
      } catch (e) {
        // connection loss rejects live iterators — expected here
      }
    },
    info => infos.push(tag + (info.slot === slot ? 'named' : 'random') + ':' + info.resumed)
  )

  const temporary = subscriber({})
  const durable = subscriber({ slot })
  await listen(temporary, 't')
  await listen(durable, 'd')

  await sql`insert into test (name) values ('a')`
  await delay(300)

  // Kill the replication connections, then write while nobody is listening:
  // the temporary slot comes back fresh, the durable one resumes and replays.
  await sql`select pg_terminate_backend(pid) from pg_stat_activity where backend_type = 'walsender'`
  while ((await sql`select active_pid from pg_replication_slots where slot_name = ${ slot }`)[0].active_pid)
    await delay(20)
  await sql`insert into test (name) values ('b')`
  await delay(1000)
  await sql`insert into test (name) values ('c')`
  await delay(300)

  await temporary.end()
  const handle = await durable.subscribe('transaction', () => { /* noop */ })
  await handle.drop()
  await durable.end()

  return [
    'dnamed:false,dnamed:true,trandom:false,trandom:false da,db,dc',
    infos.sort().join(',') + ' ' + seen.filter(x => x[0] === 'd').join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('subscribe durable slot recreates an invalidated slot', { timeout: 30 }, async() => {
  const slot = 'postgresjs_test_lost'
  const sql = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false
  })

  await sql.unsafe('create publication alltables for all tables')
  await sql`select pg_drop_replication_slot(slot_name) from pg_replication_slots where slot_name = ${ slot }`
  await sql`create table test (id serial primary key, name text)`

  const subscriber = () => postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    slot
  })

  const infos = []
      , seen = []
      , errors = []

  const listen = x => x.subscribe(
    'transaction',
    async changes => {
      try {
        for await (const c of changes)
          c.command === 'insert' && seen.push(c.row.name)
      } catch (e) {
        // connection loss rejects live iterators — expected here
      }
    },
    info => infos.push(info.resumed),
    e => errors.push(e.code)
  )

  const first = subscriber()
  const a = await listen(first)
  await sql`insert into test (name) values ('a')`
  await delay(300)
  await a.sql.end()
  await first.end()

  // History the slot retains but may no longer serve: cap what a slot can
  // hold at less than one segment, move past the slot's position, and let a
  // checkpoint invalidate it.
  await sql`insert into test (name) values ('lost')`
  try {
    await sql`alter system set max_slot_wal_keep_size = '1MB'`
    await sql`select pg_reload_conf()`
    await delay(300)
    await sql`select pg_switch_wal()`
    await sql`insert into test (name) values ('lost too')`
    await sql`select pg_switch_wal()`
    await sql`checkpoint`
  } finally {
    await sql`alter system reset max_slot_wal_keep_size`
    await sql`select pg_reload_conf()`
  }
  const [{ wal_status: before }] = await sql`select wal_status from pg_replication_slots where slot_name = ${ slot }`

  errors.length = 0
  const second = subscriber()
  const b = await listen(second)
  await sql`insert into test (name) values ('b')`
  await delay(500)
  const [{ wal_status: after }] = await sql`select wal_status from pg_replication_slots where slot_name = ${ slot }`

  await b.drop()
  await second.end()

  return [
    'lost a,b false,false reserved SLOT_INVALIDATED',
    before + ' ' + seen.join(',') + ' ' + infos.join(',') + ' ' + after + ' ' + errors.join(','),
    await sql`drop table test`,
    await sql`drop publication alltables`,
    await sql.end()
  ]
})

t('Arrays with a non-default lower bound decode without their bounds', async() => {
  const [x] = await sql`
    select '[0:1]={7,8}'::int[] as a,
           '[5:5]={9}'::int8[] as b,
           '[-1:0]={x,y}'::text[] as c,
           '[1:2][3:4]={{1,2},{3,4}}'::int[] as d,
           array['0:1]=', '{']::text[] as e,
           '{{1,2},{3,4}}'::int[] as f
  `
  return [
    '[7,8] ["9"] ["x","y"] [[1,2],[3,4]] ["0:1]=","{"] [[1,2],[3,4]]',
    [x.a, x.b, x.c, x.d, x.e, x.f].map(v => JSON.stringify(v)).join(' ')
  ]
})

t('null elements in arrays decode as null', async() => {
  const [x] = await sql`
    select '{1,NULL,3}'::int[] as ints,
           '{a,NULL,"NULL"}'::text[] as texts,
           '{{1,NULL},{NULL,4}}'::int[][] as nested
  `
  return [
    '[1,null,3]|["a",null,"NULL"]|[[1,null],[null,4]]',
    [x.ints, x.texts, x.nested].map(v => JSON.stringify(v)).join('|')
  ]
})

t('Execute', async() => {
  const result = await new Promise((resolve) => {
    const sql = postgres({ ...options, fetch_types: false, debug:(id, query) => resolve(query) })
    sql`select 1`.execute()
  })

  return [result, 'select 1']
})

t('Cancel running query', async() => {
  const query = sql`select pg_sleep(2)`
  setTimeout(() => query.cancel(), 500)
  const error = await query.catch(x => x)
  return ['57014', error.code]
})

t('Cancel piped query', { timeout: 5 }, async() => {
  await sql`select 1`
  const last = sql`select pg_sleep(1)`.execute()
  const query = sql`select pg_sleep(2) as dig`
  setTimeout(() => query.cancel(), 500)
  const error = await query.catch(x => x)
  await last
  return ['57014', error.code]
})

t('Cancel queued query', async() => {
  const query = sql`select pg_sleep(2) as nej`
  const tx = sql.begin(sql => (
    query.cancel(),
    sql`select pg_sleep(0.5) as hej, 'hejsa'`
  ))
  const error = await query.catch(x => x)
  await tx
  return ['57014', error.code]
})

t('Cancel returns the CancelRequest promise', async() => {
  let cancelling
  const query = sql`select pg_sleep(2) as nej`
  const tx = sql.begin(sql => (
    cancelling = query.cancel(),
    sql`select pg_sleep(0.5) as hej, 'hejsa'`
  ))
  const error = await query.catch(x => x)
  await tx
  await cancelling
  return ['57014 true', error.code + ' ' + (cancelling instanceof Promise)]
})

t('Cancel of a running query returns the CancelRequest promise', async() => {
  let cancelling
  const query = sql`select pg_sleep(2)`
  setTimeout(() => (cancelling = query.cancel()), 200)
  const error = await query.catch(x => x)
  // Swallowed here, not dropped: an unhandled rejection from the second
  // connection is fatal in Node by default.
  await cancelling.catch(() => { /* noop */ })
  return ['57014 true', error.code + ' ' + (cancelling instanceof Promise)]
})

t('Cancel while still connecting keeps the pool connection', async() => {
  const sql = postgres(options) // eslint-disable-line
  const query = sql`select pg_sleep(2)`
  query.execute()
  // One macrotask is enough to have the query handed to a connection and
  // parked as its `initial`, and too early for any handshake to have finished.
  await new Promise(r => setImmediate(r))
  await query.cancel()
  const error = await query.catch(x => x)

  return ['57014 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('End settles after cancelling a still connecting query', async() => {
  const sql = postgres(options) // eslint-disable-line
  const query = sql`select pg_sleep(2)`
  query.execute()
  await new Promise(r => setImmediate(r))
  await query.cancel()
  const error = await query.catch(x => x)

  // end() with the socket still connecting parks on a promise only
  // terminate() settles.
  return ['57014', error.code, await sql.end()]
})

t('Cancel before dispatch leaves the connection usable', async() => {
  const sql = postgres(options) // eslint-disable-line
  await sql`select 1`
  const query = sql`select pg_sleep(2)`
  await query.cancel()
  const error = await query.catch(x => x)

  return ['57014 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('Cancel before dispatch inside a transaction', async() => {
  const sql = postgres(options) // eslint-disable-line
  const x = await sql.begin(async sql => {
    const query = sql`select 1`
    await query.cancel()
    await query.catch(() => { /* noop */ })
    return (await sql`select 2 as x`)[0].x
  })

  return [2, x, await sql.end()]
})

t('Cancel while queued behind a cursor in a transaction', async() => {
  const sql = postgres(options) // eslint-disable-line
  let after
  // A cursor parks the connection in `full` for its whole run, so anything
  // dispatched while it walks lands in the transaction's own queue. Cancelling
  // one of those wrote nothing, so the drain must skip it rather than spend the
  // transaction's next turn on it - there is no further ReadyForQuery coming.
  // Nothing reached the server either, so the caught cancel does not fail the
  // transaction: it commits.
  const result = await sql.begin(async sql => {
    const walk = sql`select i from generate_series(1, 3) i`
      .cursor(() => delay(20))
      .execute()

    await delay(25)
    const query = sql`select 1 as one`
    query.execute()
    await new Promise(r => setImmediate(r))
    await query.cancel()
    await query.catch(() => { /* noop */ })

    await walk
    after = (await sql`select 2 as x`)[0].x
    return 'committed'
  }).catch(x => x.code)

  return ['committed 2', result + ' ' + after, await sql.end()]
})

t('A statement cancelled before it was written does not fail its transaction', async() => {
  const sql = postgres(options) // eslint-disable-line
  await sql`create table test (x int)`
  let error
  // The first run of a statement with parameters describes before it
  // executes, which parks the connection as full: the select behind it waits
  // in the transaction's own queue, and the cancel dequeues it.
  const result = await sql.begin(async sql => {
    const hold = sql`select pg_sleep(${ 0.2 })`.execute()
    await new Promise(r => setImmediate(r))
    const query = sql`select 1 as x`
    query.execute()
    await new Promise(r => setImmediate(r))
    await query.cancel()
    error = await query.catch(x => x.code)
    await hold
    await sql`insert into test values (1)`
    return 'committed'
  }).catch(x => x.code)
  const [{ n }] = await sql`select count(*)::int as n from test`

  return ['57014 committed 1', error + ' ' + result + ' ' + n, await sql`drop table test`, await sql.end()]
})

t('A statement cancelled while running still fails its transaction', async() => {
  const sql = postgres(options) // eslint-disable-line
  // The server cancelled it, so the transaction is aborted: catching the
  // cancel does not let the callback carry on into a commit.
  const result = await sql.begin(async sql => {
    const query = sql`select pg_sleep(1)`
    query.execute()
    await delay(50)
    await query.cancel()
    await query.catch(() => { /* noop */ })
    return 'committed'
  }).catch(x => x.code)

  return ['57014', result, await sql.end()]
})

t('Cancel while queued behind a cursor in a reserved connection', async() => {
  const sql = postgres(options) // eslint-disable-line
  const reserved = await sql.reserve()
  const walk = reserved`select i from generate_series(1, 3) i`
    .cursor(() => delay(20))
    .execute()

  await delay(25)
  const query = reserved`select 1 as one`
  query.execute()
  await new Promise(r => setImmediate(r))
  await query.cancel()
  await query.catch(() => { /* noop */ })

  await walk
  const x = (await reserved`select 2 as x`)[0].x
  reserved.release()

  return [2, x, await sql.end()]
})

t('A cancellable query waits off a busy pipeline and cancels at once', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  await sql`select 1`
  const hold = sql`select pg_sleep(0.5)`.execute()
  await new Promise(r => setImmediate(r))
  // Pipelined behind the sleep, the cancel would only mark it: the fast
  // select would run once the sleep ended, and resolve.
  const query = sql`select 1 as x`.cancellable().execute()
  await new Promise(r => setImmediate(r))
  const start = Date.now()
  await query.cancel()
  const error = await query.catch(x => x)
  const took = Date.now() - start
  await hold

  return ['57014 true 1', error.code + ' ' + (took < 250) + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('A pipelined query starts when the one ahead of it completes', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  await sql`select 1`
  const hold = sql`select pg_sleep(0.2)`.execute()
  await new Promise(r => setImmediate(r))
  // Written to the socket at once, behind the sleep: it is not running yet.
  const query = sql`select 1 as x`.execute()
  await new Promise(r => setImmediate(r))
  const before = query.startedAt
  await hold
  await query

  return ['0 true', before + ' ' + (query.startedAt - hold.startedAt >= 190), await sql.end()]
})

t('A query waiting for a connection starts when it gets one', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  await sql`select 1`
  const hold = sql`select pg_sleep(0.2)`.execute()
  await new Promise(r => setImmediate(r))
  const query = sql`select 1 as x`.cancellable().execute()
  await query

  return [true, query.startedAt - hold.startedAt >= 190, await sql.end()]
})

t('Stats count the pool\'s connections and the queries waiting for one', async() => {
  const sql = postgres({ ...options, max: 2 }) // eslint-disable-line
  const empty = sql.stats()
  await Promise.all([sql`select 1`, sql`select 1`])
  const one = sql.stats()
  const reserved = await sql.reserve()
  const hold = sql`select pg_sleep(0.1)`.execute()
  await new Promise(r => setImmediate(r))
  const waiter = sql`select 1`.cancellable().execute()
  await new Promise(r => setImmediate(r))
  const full = sql.stats()
  reserved.release()
  await Promise.all([hold, waiter])
  const idle = sql.stats()
  const f = x => [x.max, x.open, x.busy, x.idle, x.waiting].join()

  return [
    '2,0,0,0,0 2,2,0,2,0 2,2,2,0,1 2,2,0,2,0',
    [empty, one, full, idle].map(f).join(' '),
    await sql.end()
  ]
})

t('End with more queries than connections closes every connection', async() => {
  const application_name = 'end_' + crypto.randomBytes(4).toString('hex')
  const pool = postgres({ ...options, max: 2, connection: { application_name } })
  // Three queries on two connections: the third waits in the pool queue.
  // An ending connection used to reconnect for it and then park open.
  const xs = [1, 2, 3].map(x => pool`select ${ x }::int as x`.execute())
  await pool.end()
  const got = (await Promise.all(xs)).map(([{ x }]) => x).join()
  await delay(100)
  const [{ n }] = await sql`select count(*)::int as n from pg_stat_activity where application_name = ${ application_name }`

  return ['1,2,3 0', got + ' ' + n]
})

t('End refuses a reserve still waiting for a connection', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  const busy = sql`select pg_sleep(0.1)`.execute()
  await new Promise(r => setImmediate(r))
  const queued = sql`select 1 as x`.execute()
  const reserved = sql.reserve().then(() => 'reserved', e => e.code)
  await sql.end()

  await busy

  return ['1 CONNECTION_ENDED', (await queued)[0].x + ' ' + await reserved]
})

t('Fragments', async() => [
  1,
  (await sql`
    ${ sql`select` } 1 as x
  `)[0].x
])

t('Result becomes array', async() => [
  true,
  (await sql`select 1`).slice() instanceof Array
])

t('Describe', async() => {
  const type = (await sql`select ${ 1 }::int as x`.describe()).types[0]
  return [23, type]
})

t('Describe a statement', async() => {
  await sql`create table tester (name text, age int)`
  const r = await sql`select name, age from tester where name like $1 and age > $2`.describe()
  return [
    '25,23/name:25,age:23',
    `${ r.types.join(',') }/${ r.columns.map(c => `${c.name}:${c.type}`).join(',') }`,
    await sql`drop table tester`
  ]
})

t('Include table oid and column number in column details', async() => {
  await sql`create table tester (name text, age int)`
  const r = await sql`select name, age from tester where name like $1 and age > $2`.describe()
  const [{ oid }] = await sql`select oid from pg_class where relname = 'tester'`

  return [
    `table:${oid},number:1|table:${oid},number:2`,
    `${ r.columns.map(c => `table:${c.table},number:${c.number}`).join('|') }`,
    await sql`drop table tester`
  ]
})

t('Describe a statement without parameters', async() => {
  await sql`create table tester (name text, age int)`
  const r = await sql`select name, age from tester`.describe()
  return [
    '0,2',
    `${ r.types.length },${ r.columns.length }`,
    await sql`drop table tester`
  ]
})

t('Describe a statement without columns', async() => {
  await sql`create table tester (name text, age int)`
  const r = await sql`insert into tester (name, age) values ($1, $2)`.describe()
  return [
    '2,0',
    `${ r.types.length },${ r.columns.length }`,
    await sql`drop table tester`
  ]
})

t('Large object', async() => {
  const file = rel('index.js')
      , md5 = crypto.createHash('md5').update(fs.readFileSync(file)).digest('hex')

  const lo = await sql.largeObject()
  await new Promise(async r => fs.createReadStream(file).pipe(await lo.writable()).on('finish', r))
  await lo.seek(0)

  const out = crypto.createHash('md5')
  await new Promise(r => lo.readable().then(x => x.on('data', x => out.update(x)).on('end', r)))

  return [
    md5,
    out.digest('hex'),
    await lo.close()
  ]
})

t('Catches type serialize errors', async() => {
  const sql = postgres({
    idle_timeout,
    types: {
      text: {
        from: 25,
        to: 25,
        parse: x => x,
        serialize: () => { throw new Error('watSerialize') }
      }
    }
  })

  return [
    'watSerialize',
    (await sql`select ${ 'wat' }`.catch(e => e.message))
  ]
})

t('Catches type parse errors', async() => {
  const sql = postgres({
    idle_timeout,
    types: {
      text: {
        from: 25,
        to: 25,
        parse: () => { throw new Error('watParse') },
        serialize: x => x
      }
    }
  })

  return [
    'watParse',
    (await sql`select 'wat'`.catch(e => e.message))
  ]
})

t('Catches type serialize errors in transactions', async() => {
  const sql = postgres({
    idle_timeout,
    types: {
      text: {
        from: 25,
        to: 25,
        parse: x => x,
        serialize: () => { throw new Error('watSerialize') }
      }
    }
  })

  return [
    'watSerialize',
    (await sql.begin(sql => (
      sql`select 1`,
      sql`select ${ 'wat' }`
    )).catch(e => e.message))
  ]
})

t('Catches type parse errors in transactions', async() => {
  const sql = postgres({
    idle_timeout,
    types: {
      text: {
        from: 25,
        to: 25,
        parse: () => { throw new Error('watParse') },
        serialize: x => x
      }
    }
  })

  return [
    'watParse',
    (await sql.begin(sql => (
      sql`select 1`,
      sql`select 'wat'`
    )).catch(e => e.message))
  ]
})

t('Prevent premature end of connection in transaction', async() => {
  const sql = postgres({ max_lifetime: 0.01, idle_timeout })
  const result = await sql.begin(async sql => {
    await sql`select 1`
    await delay(20)
    await sql`select 1`
    return 'yay'
  })


  return [
    'yay',
    result
  ]
})

t('Ensure reconnect after max_lifetime with transactions', { timeout: 5 }, async() => {
  const sql = postgres({
    max_lifetime: 0.01,
    idle_timeout,
    max: 1
  })

  let x = 0
  while (x++ < 10) await sql.begin(sql => sql`select 1 as x`)

  return [true, true]
})


t('Ensure transactions throw if connection is closed dwhile there is no query', async() => {
  const sql = postgres(options)
  const x = await sql.begin(async() => {
    setTimeout(() => sql.end({ timeout: 0 }), 10)
    await new Promise(r => setTimeout(r, 200))
    return sql`select 1`
  }).catch(x => x)
  return ['CONNECTION_CLOSED', x.code]
})

t('An idle transaction\'s FATAL rejects its next query, not CONNECTION_CLOSED', async() => {
  const sql = postgres({ ...options, max: 2 }) // eslint-disable-line
  const x = await sql.begin(async sql => {
    await sql`set local idle_in_transaction_session_timeout = '100ms'`
    await delay(400)
    return sql`select 1`
  }).catch(e => e)
  const [{ y }] = await sql`select 2 as y`
  return ['25P03 2', x.code + ' ' + y, await sql.end()]
})

// Kill a backend from the suite's own connection. Pass the onclose promise of
// the pool that used it to also wait until that pool has seen it close.
async function terminate(pid, closed) {
  await sql`select pg_terminate_backend(${ pid })`
  await closed
}

async function running(pid) {
  while (!(await sql`select 1 from pg_stat_activity where pid = ${ pid } and state = 'active'`).length)
    await delay(10)
}

t('A terminated backend fails its own query with its error, not the next one', async() => {
  const sql = postgres(options) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  const query = sql`select pg_sleep(2)`.catch(e => e)
  await running(pid)
  await terminate(pid)

  return ['57P01 1', (await query).code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

// The event loop blocked past `ms` - a process frozen by a laptop's sleep or a
// serverless thaw, as the server sees it.
function freeze(ms) {
  const until = Date.now() + ms
  while (Date.now() < until)
    Math.random()
}

t('A statement answered with 57P05 runs again on a fresh connection', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  await sql`set idle_session_timeout = '100ms'`
  freeze(400)
  const [{ x, other }] = await sql`select 1 as x, pg_backend_pid() <> ${ pid } as other`
  return ['1 true', x + ' ' + other, await sql.end()]
})

t('A reserved connection answered with 57P05 fails with it', async() => {
  const sql = postgres({ ...options, max: 1 }) // eslint-disable-line
  const reserved = await sql.reserve()
  await reserved`set idle_session_timeout = '100ms'`
  freeze(400)
  const error = await reserved`select 1 as x`.catch(e => e)
  reserved.release()
  return ['57P05 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('End settles after a backend is terminated mid-query', async() => {
  const sql = postgres(options) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  const query = sql`select pg_sleep(2)`.catch(e => e)
  await running(pid)
  await terminate(pid)
  await query

  // Nothing else runs on the pool: the dead query must not be kept around
  // for end() to wait on.
  return [true, true, await sql.end()]
})

t('A query that fails partway through its rows leaves the next result whole', async() => {
  const error = await sql`select 1 / (3 - x) as x from generate_series(1, 5) x`.catch(e => e)
  const result = await sql`select 1 as x`
  return ['22012 1 1', error.code + ' ' + result.length + ' ' + (result[0] && result[0].x)]
})

t('A backend terminated partway through its rows leaves the next result whole', async() => {
  // No array types query, which would run first on the reopened connection
  // and set the row counter straight by completing.
  const sql = postgres({ ...options, fetch_types: false }) // eslint-disable-line
  const [{ pid }] = await sql`select pg_backend_pid() as pid`
  const query = sql`select x, pg_sleep(case when x = 3 then 2 else 0 end) from generate_series(1, 5) x`.catch(e => e)
  await running(pid)
  await terminate(pid)
  await query
  const result = await sql`select 1 as x`

  return ['1 1', result.length + ' ' + (result[0] && result[0].x), await sql.end()]
})

t('A transaction whose backend is terminated rejects and leaves the pool usable', async() => {
  const sql = postgres(options) // eslint-disable-line
  const error = await sql.begin(async sql => {
    const [{ pid }] = await sql`select pg_backend_pid() as pid`
    const query = sql`select pg_sleep(2)`.execute()
    await running(pid)
    await terminate(pid)
    await query
  }).catch(e => e)

  return ['CONNECTION_CLOSED 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('A transaction sends nothing more once its connection closed', async() => {
  const notices = []
  const sql = postgres({ ...options, onnotice: x => notices.push(x.code) }) // eslint-disable-line
  let done
  const finished = new Promise(r => done = r)

  const error = await sql.begin(async tx => {
    const [{ pid }] = await tx`select pg_backend_pid() as pid`
    const query = tx`select pg_sleep(2)`.execute()
    await running(pid)
    await terminate(pid)
    await query.catch(() => { /* terminated */ })
    // The pool reopens the connection for this query. Neither a statement of
    // the transaction nor the commit that returning sends may follow it into
    // that new session.
    await sql`select 1`
    done(await tx`select 1 as x`.catch(e => e.code))
  }).catch(e => e)

  const late = await finished
  await sql`select 1` // a commit sent to the new session has been answered by now
  return ['CONNECTION_CLOSED CONNECTION_CLOSED 0', error.code + ' ' + late + ' ' + notices.length, await sql.end()]
})

t('A reserved connection that closed fails its queries, and releasing it keeps the pool usable', async() => {
  let onclose
  const closed = new Promise(r => onclose = r)
  const sql = postgres({ ...options, onclose }) // eslint-disable-line
  const reserved = await sql.reserve()
  const [{ pid }] = await reserved`select pg_backend_pid() as pid`
  await terminate(pid, closed)
  const error = await reserved`select 1`.catch(e => e)
  reserved.release()

  // The server's FATAL arrived while the connection was idle: it, not a bare
  // CONNECTION_CLOSED, is why the query failed.
  return ['57P01 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('Custom socket', {}, async() => {
  let result
  const sql = postgres({
    socket: () => new Promise((resolve, reject) => {
      const socket = new net.Socket()
      socket.connect(5432)
      socket.once('data', x => result = x[0])
      socket.on('error', reject)
      socket.on('connect', () => resolve(socket))
    }),
    idle_timeout
  })

  await sql`select 1`

  return [
    result,
    82
  ]
})

t('Ensure drain only dequeues if ready', async() => {
  const sql = postgres(options)

  const res = await Promise.all([
    sql.unsafe('SELECT 0+$1 --' + '.'.repeat(100000), [1]),
    sql.unsafe('SELECT 0+$1+$2+$3', [1, 2, 3])
  ])

  return [res.length, 2]
})

t('Supports fragments as dynamic parameters', async() => {
  await sql`create table test (a int, b bool)`
  await sql`insert into test values(1, true)`
  await sql`insert into test ${
    sql({
      a: 2,
      b: sql`exists(select 1 from test where b = ${ true })`
    })
  }`

  return [
    '1,t2,t',
    (await sql`select * from test`.raw()).join(''),
    await sql`drop table test`
  ]
})

t('Supports nested fragments with parameters', async() => {
  await sql`create table test ${
    sql`(${ sql('a') } ${ sql`int` })`
  }`
  await sql`insert into test values(1)`
  return [
    1,
    (await sql`select a from test`)[0].a,
    await sql`drop table test`
  ]
})

t('Supports multiple nested fragments with parameters', async() => {
  const [{ b }] = await sql`select * ${
    sql`from ${
      sql`(values (2, ${ 1 }::int)) as x(${ sql(['a', 'b']) })`
    }`
  }`
  return [
    1,
    b
  ]
})

t('Supports arrays of fragments', async() => {
  const [{ x }] = await sql`
    ${ [sql`select`, sql`1`, sql`as`, sql`x`] }
  `

  return [
    1,
    x
  ]
})

t('Does not try rollback when commit errors', async() => {
  let notice = null
  const sql = postgres({ ...options, onnotice: x => notice = x })
  await sql`create table test(x int constraint test_constraint unique deferrable initially deferred)`

  await sql.begin('isolation level serializable', async sql => {
    await sql`insert into test values(1)`
    await sql`insert into test values(1)`
  }).catch(e => e)

  return [
    notice,
    null,
    await sql`drop table test`
  ]
})

t('Last keyword used even with duplicate keywords', async() => {
  await sql`create table test (x int)`
  await sql`insert into test values(1)`
  const [{ x }] = await sql`
    select
      1 in (1) as x
    from test
    where x in ${ sql([1, 2]) }
  `

  return [x, true, await sql`drop table test`]
})

t('Insert array with null', async() => {
  await sql`create table test (x int[])`
  await sql`insert into test ${ sql({ x: [1, null, 3] }) }`
  return [
    1,
    (await sql`select x from test`)[0].x[0],
    await sql`drop table test`
  ]
})

t('Insert array with undefined throws', async() => {
  await sql`create table test (x int[])`
  return [
    'UNDEFINED_VALUE',
    await sql`insert into test ${ sql({ x: [1, undefined, 3] }) }`.catch(e => e.code),
    await sql`drop table test`
  ]
})

t('Insert array with undefined transform', async() => {
  const sql = postgres({ ...options, transform: { undefined: null } })
  await sql`create table test (x int[])`
  await sql`insert into test ${ sql({ x: [1, undefined, 3] }) }`
  return [
    1,
    (await sql`select x from test`)[0].x[0],
    await sql`drop table test`
  ]
})

t('concurrent cursors', async() => {
  const xs = []

  await Promise.all([...Array(7)].map((x, i) => [
    sql`select ${ i }::int as a, generate_series(1, 2) as x`.cursor(([x]) => xs.push(x.a + x.x))
  ]).flat())

  return ['12233445566778', xs.join('')]
})

t('concurrent cursors multiple connections', async() => {
  const sql = postgres({ ...options, max: 2 })
  const xs = []

  await Promise.all([...Array(7)].map((x, i) => [
    sql`select ${ i }::int as a, generate_series(1, 2) as x`.cursor(([x]) => xs.push(x.a + x.x))
  ]).flat())

  return ['12233445566778', xs.sort().join('')]
})

t('reserve connection', async() => {
  const reserved = await sql.reserve()

  setTimeout(() => reserved.release(), 510)

  const xs = await Promise.all([
    reserved`select 1 as x`.then(([{ x }]) => ({ time: Date.now(), x })),
    sql`select 2 as x`.then(([{ x }]) => ({ time: Date.now(), x })),
    reserved`select 3 as x`.then(([{ x }]) => ({ time: Date.now(), x }))
  ])

  if (xs[1].time - xs[2].time < 500)
    throw new Error('Wrong time')

  return [
    '123',
    xs.map(x => x.x).join('')
  ]
})

t('arrays in reserved connection', async() => {
  const reserved = await sql.reserve()
  const [{ x }] = await reserved`select array[1, 2, 3] as x`
  reserved.release()

  return [
    '123',
    x.join('')
  ]
})

t('reserve connection with fetch_types disabled', async() => {
  const sql = postgres({ ...options, fetch_types: false }) // eslint-disable-line
  const reserved = await sql.reserve()
  const [{ x }] = await reserved`select 1 as x`
  reserved.release()

  return [1, x, await sql.end()]
})

t('Ensure reserve on query throws proper error', async() => {
  const sql = postgres({ idle_timeout }) // eslint-disable-line
  const reserved = await sql.reserve()
  const [{ x }] = await reserved`select 'wat' as x`

  return [
    'wat', x, reserved.release()
  ]
})

t('A query during a copy waits for it to end', async() => {
  const sql = postgres(options) // eslint-disable-line
  await sql`create table test (id serial primary key, name text)`
  const copy = await sql`copy test from stdin`.writable()
  let settled = false
  const after = sql`select count(*)::int as n from test`.then(x => (settled = true, x))
  copy.write('1\tone\n')
  await delay(50)
  const waited = !settled
  await new Promise(r => copy.end(r))
  const [{ n }] = await after

  return [
    'true 1', waited + ' ' + n,
    await sql`drop table test`
  ]
})

t('A transaction runs its statements around a copy, in the order they came', async() => {
  const sql = postgres({ ...options, max: 2 })
  await sql`create table test (id int)`
  const [counts, written] = await sql.begin(async sql => {
    const copy = await sql`copy test from stdin`.writable()
    // Issued while the copy runs (a query dispatches on its first then):
    // queued behind it, never written into it.
    const counts = Promise.all([sql`select count(*)::int as n from test`, sql`select 1 as x`])
    copy.write('1\n2\n')
    await delay(20)
    await new Promise(r => copy.end(r))
    // And another copy right behind the first, with a statement beside it.
    const second = await sql`copy test from stdin`.writable()
    const written = sql`select count(*)::int as n from test`.then(x => x)
    second.write('3\n')
    await new Promise(r => second.end(r))
    return [await counts, await written]
  })

  return [
    '2 1 3', counts[0][0].n + ' ' + counts[1][0].x + ' ' + written[0].n,
    await sql`drop table test`,
    await sql.end()
  ]
})

t('A released reservation refuses every later query and never reaches the connection again', async() => {
  const sql = postgres(options) // eslint-disable-line
  await sql`create table if not exists stale_reserve (x text)`
  const stale = await sql.reserve()
  await stale`select 1`
  stale.release()

  // max: 1, so the next reservation is the same connection, in its own
  // transaction; the stale handle's rollback must not end it.
  const next = await sql.reserve()
  await next`begin`
  await next`insert into stale_reserve values ('kept')`
  const error = await stale`rollback`.catch(e => e)
  await next`commit`
  next.release()
  const stale2 = await stale`select 1`.catch(e => e.code)
  stale.release() // a second release is a no-op

  const [{ count }] = await sql`select count(*)::int from stale_reserve where x = 'kept'`
  await sql`drop table stale_reserve`
  return ['RESERVATION_RELEASED RESERVATION_RELEASED 1', error.code + ' ' + stale2 + ' ' + count, await sql.end()]
})

t('A transaction scope that escaped its callback refuses every later query', async() => {
  const sql = postgres(options) // eslint-disable-line
  await sql`create table if not exists stale_begin (x text)`
  let escaped
  await sql.begin(async sql => {
    escaped = sql
    await sql`select 1`
  })

  const other = await sql.reserve()
  await other`begin`
  await other`insert into stale_begin values ('kept')`
  const error = await escaped`rollback`.catch(e => e)
  await other`commit`
  other.release()

  const [{ count }] = await sql`select count(*)::int from stale_begin where x = 'kept'`
  await sql`drop table stale_begin`
  return ['TRANSACTION_ENDED 1', error.code + ' ' + count, await sql.end()]
})

t('A failed transaction scope refuses later queries too', async() => {
  const sql = postgres(options) // eslint-disable-line
  let escaped
  await sql.begin(async sql => {
    escaped = sql
    throw new Error('boom')
  }).catch(() => null)
  const error = await escaped`select 1`.catch(e => e)
  return ['TRANSACTION_ENDED 1', error.code + ' ' + (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('A script whose pool sits idle exits without end()', { timeout: 20 }, async() => {
  // Only the Node socket can be unref'd; the Deno build's polyfill cannot.
  if (globalThis.Deno)
    return [1, 1]

  const { spawn } = await import('child_process')
  const source = String(rel('../src/index.js'))
  const script = `
    const { default: postgres } = await import(${ JSON.stringify(source) })
    const sql = postgres(${ JSON.stringify({ ...options, idle_timeout: null, max: 2 }) })
    const [{ x }] = await sql\`select 1 as x\`
    const reserved = await sql.reserve()
    await reserved\`select 1\`
    reserved.release()
    await sql.begin(sql => sql\`select 1\`)
    console.log(x)
  `
  const started = Date.now()
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'inherit'] })
  let out = ''
  child.stdout.on('data', x => out += x)
  const code = await new Promise(r => {
    const timer = setTimeout(() => child.kill(), 10000)
    child.on('exit', code => (clearTimeout(timer), r(code)))
  })
  return ['0 1 fast', code + ' ' + out.trim() + ' ' + (Date.now() - started < 8000 ? 'fast' : 'slow')]
})

t('end() still resolves for a connection that goes idle while the pool ends', { timeout: 20 }, async() => {
  if (globalThis.Deno)
    return [1, 1]

  const { spawn } = await import('child_process')
  const source = String(rel('../src/index.js'))
  const script = `
    const { default: postgres } = await import(${ JSON.stringify(source) })
    const sql = postgres(${ JSON.stringify({ ...options, idle_timeout: null, max: 2 }) })
    await sql\`select 1\`
    const reserved = await sql.reserve()
    await reserved\`begin\`
    const ended = sql.end()
    await new Promise(r => setTimeout(r, 200))
    await reserved\`rollback\`
    reserved.release()
    await ended
    console.log('ended')
  `
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'inherit'] })
  let out = ''
  child.stdout.on('data', x => out += x)
  const code = await new Promise(r => {
    const timer = setTimeout(() => child.kill(), 10000)
    child.on('exit', code => (clearTimeout(timer), r(code)))
  })
  return ['0 ended', code + ' ' + out.trim()]
})

t('A LISTEN connection keeps the process alive', { timeout: 20 }, async() => {
  if (globalThis.Deno)
    return [1, 1]

  const { spawn } = await import('child_process')
  const source = String(rel('../src/index.js'))
  const script = `
    const { default: postgres } = await import(${ JSON.stringify(source) })
    const sql = postgres(${ JSON.stringify({ ...options, idle_timeout: null }) })
    await sql.listen('stay_alive', () => (console.log('notified'), sql.end()))
    console.log('listening')
  `
  const child = spawn(process.execPath, ['--input-type=module', '-e', script], { stdio: ['ignore', 'pipe', 'inherit'] })
  let out = ''
  const listening = new Promise(r => child.stdout.on('data', x => (out += x, out.includes('listening') && r())))
  const exited = new Promise(r => child.on('exit', r))
  await listening
  await delay(500)
  await sql`select pg_notify('stay_alive', 'x')`
  const timer = setTimeout(() => child.kill(), 10000)
  const code = await exited
  clearTimeout(timer)
  return ['0 listening notified', code + ' ' + out.trim().split('\n').join(' ')]
})

// A TCP proxy in front of the test server that can stall its open
// connections - stop forwarding both ways without closing anything, a
// half-open link as a partition or a dropped NAT entry leaves it. Connections
// opened after the stall pass through. `turns` counts client-to-server
// chunks, one per round trip the driver starts.
async function stallingProxy() {
  const pairs = new Set()
  const proxy = {
    turns: 0,
    stall() {
      pairs.forEach(p => p.stalled = true)
    },
    close() {
      pairs.forEach(p => (p.a.destroy(), p.b.destroy()))
      return new Promise(r => server.close(r))
    }
  }
  const server = net.createServer(a => {
    const b = net.connect(process.env.PGPORT || 5432, process.env.PGHOST || 'localhost') // eslint-disable-line
        , pair = { a, b, stalled: false }
    pairs.add(pair)
    a.on('data', x => pair.stalled || (proxy.turns++, b.write(x)))
    b.on('data', x => pair.stalled || a.write(x))
    a.on('error', () => { /* noop */ })
    b.on('error', () => { /* noop */ })
    a.on('close', () => (pairs.delete(pair), b.destroy()))
    b.on('close', () => (pairs.delete(pair), a.destroy()))
  })
  await new Promise(r => server.listen(0, '127.0.0.1', r))
  proxy.port = server.address().port
  return proxy
}

t('A cancelled statement on a connection that stopped answering loses the connection', async() => {
  const proxy = await stallingProxy()
  const sql = postgres({ ...options, host: '127.0.0.1', port: proxy.port, cancel_timeout: 0.3 }) // eslint-disable-line
  await sql`select 1`
  proxy.stall()
  const query = sql`select 2 as x`.execute()
  await delay(50)
  const start = Date.now()
  query.cancel().catch(() => { /* the cancel's own connection may fail; the grace still runs */ })
  const error = await query.catch(x => x)
  const took = Date.now() - start
  // The pool replaced it: the next statement opens a fresh connection.
  const x = (await sql`select 3 as x`)[0].x
  await sql.end({ timeout: 0 })
  await proxy.close()
  return ['CONNECTION_CLOSED true 3', error.code + ' ' + (took < 1000) + ' ' + x]
})

t('A cancelled statement on a stalled connection waits without cancel_timeout', async() => {
  const proxy = await stallingProxy()
  const sql = postgres({ ...options, host: '127.0.0.1', port: proxy.port, cancel_timeout: 0 }) // eslint-disable-line
  await sql`select 1`
  proxy.stall()
  const query = sql`select 2 as x`.execute()
  query.catch(() => { /* settled by the teardown */ })
  await delay(50)
  query.cancel().catch(() => { /* noop */ })
  await delay(600)
  const settled = query.settled
  await sql.end({ timeout: 0 })
  await proxy.close()
  return [false, settled]
})

t('subscribe loses a stream that stopped answering and re-establishes it', { timeout: 10 }, async() => {
  const proxy = await stallingProxy()
  const sql = postgres({ database: 'postgres_js_test' })
  // Only the replication connection goes through the proxy: this pool never
  // opens one of its own.
  const through = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    host: '127.0.0.1',
    port: proxy.port,
    subscribe_timeout: 0.6
  })
  await sql.unsafe('create publication alltables for all tables')
  const errors = []
  let onsubscribes = 0
  const { unsubscribe } = await through.subscribe(
    'transaction',
    () => { /* noop */ },
    () => onsubscribes++,
    e => errors.push(e.code)
  )
  // Quiet but healthy: the heartbeat's reply keeps the stream alive past
  // the timeout.
  await delay(1000)
  const before = errors.length + ' ' + onsubscribes
  proxy.stall()
  while (onsubscribes < 2) // eslint-disable-line
    await delay(20)
  await unsubscribe()
  const result = before + ' ' + errors[0] + ' ' + onsubscribes
  await sql`drop publication alltables`
  await sql.end()
  await through.end({ timeout: 0 })
  await proxy.close()
  return ['0 1 SUBSCRIPTION_TIMEOUT 2', result]
})

t('An unprepared statement is described once per text, then runs in one round trip', async() => {
  const proxy = await stallingProxy()
  const sql = postgres({ ...options, host: '127.0.0.1', port: proxy.port, prepare: false }) // eslint-disable-line
  await sql`select 1`
  const turns = async q => {
    const before = proxy.turns
    await q
    return proxy.turns - before
  }
  const first = await turns(sql`select ${ 'a' } as x`)
  const second = await turns(sql`select ${ 'b' } as x`)
  // Every type pinned by the values: never described first.
  const pinned = await turns(sql`select ${ true } as x, ${ new Date(0) } as y`)
  await sql.end()
  await proxy.close()
  return ['2 1 1', first + ' ' + second + ' ' + pinned]
})

t('A reused description that no longer fits is described again and run', async() => {
  const sql = postgres({ ...options, prepare: false }) // eslint-disable-line
  await sql`create table test (x int)`
  await sql`insert into test values (1)`
  const a = await sql`select x from test where x = ${ '1' }`
  await sql`alter table test alter x type text`
  await sql`insert into test values ('b')`
  // Described before as int4: 'b' fails to bind as that, nothing ran, and the
  // statement is described afresh and run as text.
  const b = await sql`select x from test where x = ${ 'b' }`
  return ['1 b', a[0].x + ' ' + b[0].x, await sql`drop table test`, await sql.end()]
})

t('The first call of a statement does not pin its arguments', async() => {
  // The origin of a tagged query is captured once per strings array. An
  // unformatted V8 stack keeps its frames' functions alive, and with them
  // what their closures hold: here the first call's argument.
  const v8 = await import('v8')
  const vm = await import('vm')
  v8.setFlagsFromString('--expose-gc')
  const gc = vm.runInNewContext('gc')
  const strings = Object.assign(['select length(', '::bytea) as x'], { raw: ['select length(', '::bytea) as x'] })
  let ref
  const first = async() => {
    const held = { bytes: Buffer.alloc(1 << 20) }
    const build = () => sql(strings, held.bytes)
    ref = new globalThis.WeakRef(held)
    return (await build())[0].x
  }
  const length = await first()
  await sql(strings, Buffer.alloc(1))
  await delay(10)
  gc()
  await delay(10)
  gc()
  return [true, length === 1 << 20 && ref.deref() === undefined]
})

t('Queries, connects and transactions keep real time once the timer globals are faked', async() => {
  // What vi.useFakeTimers() and friends do after the driver is loaded: every
  // timer global replaced by one that never fires. Faking setImmediate hung
  // every query (the write batching), setTimeout every new connection.
  const names = ['setTimeout', 'clearTimeout', 'setInterval', 'clearInterval', 'setImmediate', 'clearImmediate']
      , saved = names.map(name => globalThis[name])
      , realDelay = saved[0]
      , now = Date.now
  let result
  try {
    names.forEach(name => globalThis[name] = () => 0)
    Date.now = () => 0
    const fresh = postgres({ ...options, max: 2, idle_timeout: 0.05 }) // eslint-disable-line
    const [{ x }] = await fresh`select 1 as x`
    const [{ y }] = await fresh.begin(sql => sql`select 2 as y`)
    await new Promise(r => realDelay(r, 120))
    // The idle timeout fired on the real clock: this query reconnects.
    const [{ z }] = await fresh`select 3 as z`
    await fresh.end({ timeout: 1 })
    result = '' + x + y + z
  } finally {
    names.forEach((name, i) => saved[i] !== undefined && (globalThis[name] = saved[i]))
    Date.now = now
  }
  return ['123', result]
})

t('subscribe reports how long its stream has been quiet, keepalives included', { timeout: 10 }, async() => {
  const sql = postgres({ database: 'postgres_js_test' })
  const stream = postgres({
    database: 'postgres_js_test',
    publications: 'alltables',
    fetch_types: false,
    subscribe_timeout: 0.6
  })
  await sql.unsafe('create publication alltables for all tables')
  const handle = await stream.subscribe('transaction', () => { /* noop */ })
  const first = handle.quietMs
  // No writes: the watchdog's reply requests every 200 ms bring keepalives,
  // so an idle healthy stream never reads quiet for long.
  let most = 0
  for (let i = 0; i < 10; i++) {
    await delay(100)
    most = Math.max(most, handle.quietMs)
  }
  handle.unsubscribe()
  await sql`drop publication alltables`
  await sql.end()
  await stream.end({ timeout: 0 })
  return [true, typeof first === 'number' && first >= 0 && most < 500]
})

// A socket to the test server whose outgoing chunks pass through `rewrite`,
// opened after `wait` ms - for the n-th connection the pool, or a
// CancelRequest, opens.
const through = ({ rewrite = x => x, wait = () => 0 } = {}) => {
  let n = 0
  return async() => {
    const ms = wait(n++)
    ms && await delay(ms)
    return new Promise((resolve, reject) => {
      const socket = net.connect(Number(process.env.PGPORT || 5432), process.env.PGHOST || 'localhost') // eslint-disable-line
      const write = socket.write.bind(socket)
      socket.write = (chunk, ...rest) => write(rewrite(chunk), ...rest)
      socket.on('error', reject)
      socket.on('connect', () => resolve(socket))
    })
  }
}

// A simple Query message: what a rewriting socket sends instead.
const simpleQuery = text => {
  const body = Buffer.from(text + '\0')
  const head = Buffer.alloc(5)
  head[0] = 81 // Q
  head.writeInt32BE(body.length + 4, 1)
  return Buffer.concat([head, body])
}

t('A CancelRequest still in flight does not cancel the next statement on its connection', { timeout: 5 }, async() => {
  // Every connection after the pool's one is a CancelRequest, and it reaches
  // the server late: after the statement it was for finished on its own. The
  // next statement on the connection waits for it to land; sent at once, it
  // was the one the late signal cancelled.
  const sql = postgres({ ...options, socket: through({ wait: n => n > 0 && 300 }) })
  await sql`select 1`
  const first = sql`select pg_sleep(0.05)`
  first.execute()
  await delay(20)
  const cancelling = first.cancel()
  await first
  const next = await sql`select pg_sleep(0.5), 1 as x`.then(x => x[0].x, e => e.code)
  await cancelling
  return [1, next, await sql.end()]
})

t('A statement held behind a CancelRequest and cancelled itself leaves the connection usable', { timeout: 5 }, async() => {
  const sql = postgres({ ...options, socket: through({ wait: n => n > 0 && 200 }) })
  await sql`select 1`
  const first = sql`select pg_sleep(0.05)`
  first.execute()
  await delay(20)
  const cancelling = first.cancel()
  await first
  const held = sql`select 2`
  held.execute()
  await delay(20)
  await held.cancel()
  const error = await held.catch(e => e.code)
  await cancelling
  return ['57014 3', error + ' ' + (await sql`select 3 as x`)[0].x, await sql.end()]
})

t('A COMMIT that leaves its transaction aborted closes the connection', { timeout: 5 }, async() => {
  // The COMMIT is swapped on the wire for a statement that cancels itself: a
  // cancel that lands at the start of COMMIT does the same - 57014, and the
  // session left in its transaction, aborted. Kept reserved, the pool of one
  // never served again; closed, the server rolls the insert back.
  let armed = false
  const sql = postgres({
    ...options,
    socket: through({
      rewrite: x => armed && x.includes('commit\0')
        ? (armed = false, simpleQuery('select pg_cancel_backend(pg_backend_pid())'))
        : x
    })
  })
  await sql`create table test (x int)`
  armed = true
  const error = await sql.begin(sql => sql`insert into test values (1)`).catch(e => e.code)
  const [{ n }] = await sql.begin(sql => sql`select count(*)::int as n from test`)
  await sql`drop table test`
  return ['57014 0', error + ' ' + n, await sql.end()]
})

t('A reserved connection released inside a transaction is closed, not pooled', async() => {
  const sql = postgres(options)
  const reserved = await sql.reserve()
  await reserved`begin`
  await reserved`select 1/0`.catch(() => { /* noop */ })
  reserved.release()
  return [1, (await sql`select 1 as x`)[0].x, await sql.end()]
})

t('The stream reconnect backoff stays within its ceiling', async() => {
  const delays = Array.from({ length: 64 }, (_, i) => reconnectDelay(i))
  return ['50 1000 1000', delays[0] + ' ' + Math.min(...delays.slice(5)) + ' ' + Math.max(...delays)]
})

t('A reserve() waiting when a connection closes gets the reopened one', { timeout: 5 }, async() => {
  const sql = postgres(options)
  const first = await sql.reserve()
  const second = sql.reserve()
  await delay(20)
  await first`select pg_terminate_backend(pg_backend_pid())`.catch(() => { /* noop */ })
  const reserved = await second
  const [{ x }] = await reserved`select 1 as x`
  reserved.release()
  return [1, x, await sql.end()]
})

t('A subscribe that fails is retried by the next one, not replayed', { timeout: 10 }, async() => {
  const sql = postgres({ database: 'postgres_js_test' })
  const stream = postgres({ database: 'postgres_js_test', publications: 'alltables', fetch_types: false })
  await sql.unsafe('create publication alltables for all tables')
  // Every free slot taken by another consumer: CREATE_REPLICATION_SLOT
  // fails 53400 - an ERROR, so the replication connection stays open and
  // nothing re-establishes.
  const [{ max }] = await sql`select current_setting('max_replication_slots')::int as max`
  const [{ used }] = await sql`select count(*)::int as used from pg_replication_slots`
  const taken = Array.from({ length: max - used }, (_, i) => 'pgjs_taken_' + i)
  for (const name of taken)
    await sql`select pg_create_logical_replication_slot(${ name }, 'pgoutput')`
  const failed = await stream.subscribe('transaction', () => { /* noop */ }).then(() => 'subscribed', e => e.code)
  await sql`select pg_drop_replication_slot(${ taken.pop() })`
  const handle = await stream.subscribe('transaction', () => { /* noop */ })
  handle.unsubscribe()
  await stream.end({ timeout: 0 })
  for (const name of taken)
    await sql`select pg_drop_replication_slot(${ name })`
  await sql`drop publication alltables`
  await sql.end()
  return ['53400 true', failed + ' ' + (typeof handle.unsubscribe === 'function')]
})
