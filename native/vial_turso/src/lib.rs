//! Turso driver NIF for VialKeeper's physical storage backend.
//!
//! The function set and term encoding match `native/vial_sqlite`, so the
//! Elixir connection layer can drive either engine. Every call that touches
//! the database runs on a dirty IO scheduler and blocks on one shared tokio
//! runtime. `query_inline/3` always answers `:contended`, which sends the
//! caller to the dirty path.
//!
//! All connections opened for one path in this OS process share one
//! `turso::Database`, because MVCC state lives in the database object.

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard, OnceLock, Weak};
use std::time::Duration;

use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, ResourceArc, Term, TermType};
use turso::{Builder, Connection, Database, Value};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        blob,
        closed,
        contended,
        unsupported,
        write_conflict,
    }
}

const DEFAULT_BUSY_TIMEOUT_MS: u64 = 2000;
const SQLITE_OPEN_CREATE: i32 = 0x4;

static RUNTIME: OnceLock<tokio::runtime::Runtime> = OnceLock::new();
static DATABASES: OnceLock<Mutex<HashMap<PathBuf, Weak<Database>>>> = OnceLock::new();

fn runtime() -> &'static tokio::runtime::Runtime {
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .thread_name("vial_turso")
            .enable_all()
            .build()
            .expect("failed to start the Turso runtime")
    })
}

pub struct Conn {
    // Dropped before `database`, which owns the shared MVCC state. `close/1`
    // drops both, so a closed connection never keeps the database object (and
    // its in-memory MVCC state) alive.
    db: Mutex<Option<Connection>>,
    database: Mutex<Option<Arc<Database>>>,
}

#[rustler::resource_impl]
impl rustler::Resource for Conn {}

type Reply<'a> = Result<Term<'a>, Term<'a>>;

fn turso_error<'a>(env: Env<'a>, error: turso::Error) -> Term<'a> {
    match error {
        turso::Error::Busy(_) | turso::Error::BusySnapshot(_) => {
            error_term(env, atoms::write_conflict())
        }
        other => {
            let message = other.to_string();

            if message.to_ascii_lowercase().contains("conflict") {
                error_term(env, atoms::write_conflict())
            } else {
                error_term(env, message)
            }
        }
    }
}

fn error_term<'a>(env: Env<'a>, reason: impl Encoder) -> Term<'a> {
    (atoms::error(), reason).encode(env)
}

fn lock(conn: &Conn) -> MutexGuard<'_, Option<Connection>> {
    conn.db
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

fn with_db<'a>(
    env: Env<'a>,
    conn: &Conn,
    fun: impl FnOnce(&Connection) -> Reply<'a>,
) -> Term<'a> {
    match lock(conn).as_ref() {
        None => error_term(env, atoms::closed()),
        Some(db) => match fun(db) {
            Ok(term) | Err(term) => term,
        },
    }
}

/// Returns the process-wide database for `path`, opening it on first use.
fn shared_database(path: &str) -> turso::Result<Arc<Database>> {
    let key = std::fs::canonicalize(path).unwrap_or_else(|_| PathBuf::from(path));
    let registry = DATABASES.get_or_init(|| Mutex::new(HashMap::new()));
    let mut databases = registry
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner());

    // A live entry for a file that no longer exists belongs to a deleted
    // database; a new file at the same path gets a fresh database object.
    if std::path::Path::new(path).exists() {
        if let Some(database) = databases.get(&key).and_then(Weak::upgrade) {
            return Ok(database);
        }
    }

    let database = Arc::new(runtime().block_on(
        Builder::new_local(path).build(),
    )?);

    // An open on a new file creates it, so canonicalize again for the key.
    let key = std::fs::canonicalize(path).unwrap_or(key);
    databases.retain(|_, weak| weak.strong_count() > 0);
    databases.insert(key, Arc::downgrade(&database));
    Ok(database)
}

#[rustler::nif(schedule = "DirtyIo")]
fn open<'a>(env: Env<'a>, path: String, flags: i32) -> Term<'a> {
    // Readers share the writer's database object, so it always opens
    // read-write; `PRAGMA query_only` keeps reader connections from writing.
    let in_memory = path == ":memory:";

    if !in_memory && flags & SQLITE_OPEN_CREATE == 0 && !std::path::Path::new(&path).exists() {
        return error_term(env, "unable to open database file");
    }

    // A memory database is private to its connection, as in SQLite.
    let database = if in_memory {
        runtime().block_on(Builder::new_local(&path).build()).map(Arc::new)
    } else {
        shared_database(&path)
    };

    let database = match database {
        Ok(database) => database,
        Err(error) => return turso_error(env, error),
    };

    let db = match database.connect() {
        Ok(db) => db,
        Err(error) => return turso_error(env, error),
    };

    if let Err(error) = db.busy_timeout(Duration::from_millis(DEFAULT_BUSY_TIMEOUT_MS)) {
        return turso_error(env, error);
    }

    let conn = Conn {
        db: Mutex::new(Some(db)),
        database: Mutex::new(Some(database)),
    };

    (atoms::ok(), ResourceArc::new(conn)).encode(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn close(env: Env, conn: ResourceArc<Conn>) -> Term {
    drop(lock(&conn).take());
    drop(
        conn.database
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .take(),
    );
    atoms::ok().encode(env)
}

/// Turso has no interrupt handle; a running statement finishes on its own.
#[rustler::nif]
fn cancel(_conn: ResourceArc<Conn>) -> rustler::Atom {
    atoms::ok()
}

#[rustler::nif(schedule = "DirtyIo")]
fn set_busy_timeout(conn: ResourceArc<Conn>, timeout_ms: i32) -> rustler::Atom {
    if let Some(db) = lock(&conn).as_ref() {
        let _ = db.busy_timeout(Duration::from_millis(timeout_ms.max(0) as u64));
    }

    atoms::ok()
}

/// Runs SQL text that may hold several statements and returns no rows.
#[rustler::nif(schedule = "DirtyIo")]
fn execute<'a>(env: Env<'a>, conn: ResourceArc<Conn>, sql: String) -> Term<'a> {
    with_db(env, &conn, |db| {
        runtime()
            .block_on(db.execute_batch(&sql))
            .map(|()| atoms::ok().encode(env))
            .map_err(|error| turso_error(env, error))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn query<'a>(env: Env<'a>, conn: ResourceArc<Conn>, sql: Binary<'a>, params: Term<'a>) -> Term<'a> {
    with_db(env, &conn, |db| run(env, db, sql, params))
}

/// Always `:contended`: every Turso statement takes the dirty path.
#[rustler::nif]
fn query_inline<'a>(
    env: Env<'a>,
    _conn: ResourceArc<Conn>,
    _sql: Binary<'a>,
    _params: Term<'a>,
) -> Term<'a> {
    atoms::contended().encode(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn last_insert_rowid(env: Env, conn: ResourceArc<Conn>) -> Term {
    with_db(env, &conn, |db| Ok((atoms::ok(), db.last_insert_rowid()).encode(env)))
}

#[rustler::nif]
fn serialize(env: Env, _conn: ResourceArc<Conn>) -> Term {
    error_term(env, atoms::unsupported())
}

fn run<'a>(env: Env<'a>, db: &Connection, sql: Binary<'a>, params: Term<'a>) -> Reply<'a> {
    let sql =
        std::str::from_utf8(sql.as_slice()).map_err(|_| error_term(env, "invalid SQL text"))?;
    let params = bind(env, params)?;

    let rows: turso::Result<Vec<Vec<Value>>> = runtime().block_on(async {
        let mut statement = db.prepare_cached(sql).await?;
        let mut rows = statement.query(params).await?;
        let columns = rows.column_count();
        let mut out = Vec::new();

        while let Some(row) = rows.next().await? {
            let mut values = Vec::with_capacity(columns);

            for index in 0..columns {
                values.push(row.get_value(index)?);
            }

            out.push(values);
        }

        Ok(out)
    });

    let rows = rows.map_err(|error| turso_error(env, error))?;
    let encoded: Vec<Term<'a>> = rows
        .iter()
        .map(|row| {
            row.iter()
                .map(|cell| value(env, cell))
                .collect::<Vec<Term<'a>>>()
                .encode(env)
        })
        .collect();

    Ok((atoms::ok(), encoded).encode(env))
}

fn bind<'a>(env: Env<'a>, params: Term<'a>) -> Result<Vec<Value>, Term<'a>> {
    let params: ListIterator<'a> = params
        .decode()
        .map_err(|_| error_term(env, "parameters must be a list"))?;

    params
        .enumerate()
        .map(|(offset, param)| {
            param_value(param)
                .ok_or_else(|| error_term(env, format!("unsupported parameter at {}", offset + 1)))
        })
        .collect()
}

/// Integers, floats, binaries (bound as text), `nil` and `{:blob, binary}`.
fn param_value(param: Term<'_>) -> Option<Value> {
    let value = match param.get_type() {
        TermType::Integer => Value::Integer(param.decode::<i64>().ok()?),
        TermType::Float => Value::Real(param.decode::<f64>().ok()?),
        TermType::Binary => {
            let bytes = param.decode::<Binary>().ok()?;
            Value::Text(String::from_utf8(bytes.as_slice().to_vec()).ok()?)
        }
        TermType::Atom if param == atoms::nil().to_term(param.get_env()) => Value::Null,
        TermType::Tuple => match rustler::types::tuple::get_tuple(param).ok()?.as_slice() {
            [tag, blob] if *tag == atoms::blob().to_term(param.get_env()) => {
                Value::Blob(blob.decode::<Binary>().ok()?.as_slice().to_vec())
            }
            _ => return None,
        },
        _ => return None,
    };

    Some(value)
}

fn value<'a>(env: Env<'a>, value: &Value) -> Term<'a> {
    match value {
        Value::Null => atoms::nil().encode(env),
        Value::Integer(integer) => integer.encode(env),
        Value::Real(real) => real.encode(env),
        Value::Text(text) => binary(env, text.as_bytes()),
        Value::Blob(bytes) => binary(env, bytes),
    }
}

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut binary = NewBinary::new(env, bytes.len());
    binary.as_mut_slice().copy_from_slice(bytes);
    Binary::from(binary).to_term(env)
}

rustler::init!("Elixir.VialKeeper.Storage.Turso.Native");
