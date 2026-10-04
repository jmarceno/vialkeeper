//! SQLite driver NIF for VialKeeper's physical storage backend.
//!
//! Each statement is one NIF call: prepare (from the connection's statement
//! cache), bind, step to completion and encode the rows. `query/3` runs on a
//! dirty IO scheduler; `query_inline/3` runs the same work on the calling
//! scheduler, for bounded statements whose dirty-scheduler hop costs more than
//! the SQLite work itself.

use std::ffi::{c_int, c_void};
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::sync::{Mutex, MutexGuard, TryLockError};
use std::time::Duration;

use rusqlite::types::{ToSqlOutput, ValueRef};
use rusqlite::{ffi, CachedStatement, Connection, InterruptHandle, OpenFlags, MAIN_DB};
use rustler::{Binary, Encoder, Env, ListIterator, NewBinary, ResourceArc, Term, TermType};

mod atoms {
    rustler::atoms! {
        ok,
        error,
        nil,
        blob,
        closed,
        contended,
    }
}

const STATEMENT_CACHE_CAPACITY: usize = 256;
const DEFAULT_BUSY_TIMEOUT_MS: i32 = 2000;
const BUSY_DELAYS_MS: [i32; 11] = [1, 2, 5, 10, 15, 20, 25, 25, 25, 50, 50];
const BUSY_MAX_DELAY_MS: i32 = 50;

/// State the busy handler reads while SQLite holds the connection.
struct Shared {
    cancelled: AtomicBool,
    busy_timeout_ms: AtomicI32,
}

pub struct Conn {
    // Dropped before `shared`, which the busy handler points at.
    db: Mutex<Option<Connection>>,
    interrupt: InterruptHandle,
    shared: Box<Shared>,
}

#[rustler::resource_impl]
impl rustler::Resource for Conn {}

impl Drop for Conn {
    fn drop(&mut self) {
        self.shared.cancelled.store(true, Ordering::Release);
    }
}

type Reply<'a> = Result<Term<'a>, Term<'a>>;

fn sqlite_error<'a>(env: Env<'a>, error: rusqlite::Error) -> Term<'a> {
    let message = match error {
        rusqlite::Error::SqliteFailure(_, Some(message)) => message,
        other => other.to_string(),
    };
    (atoms::error(), message).encode(env)
}

fn error_term<'a>(env: Env<'a>, reason: impl Encoder) -> Term<'a> {
    (atoms::error(), reason).encode(env)
}

/// SQLite's default busy handler sleeps without looking at anything else.
/// This one sleeps in short steps (the same schedule SQLite uses) and stops as
/// soon as the connection is cancelled, so `cancel/1` wakes a blocked caller.
unsafe extern "C" fn busy_handler(arg: *mut c_void, count: c_int) -> c_int {
    let shared = &*(arg as *const Shared);

    if shared.cancelled.load(Ordering::Acquire) {
        return 0;
    }

    let timeout = shared.busy_timeout_ms.load(Ordering::Relaxed);
    let count = count.max(0) as usize;
    let waited: i32 = BUSY_DELAYS_MS.iter().take(count).sum::<i32>()
        + count.saturating_sub(BUSY_DELAYS_MS.len()) as i32 * BUSY_MAX_DELAY_MS;

    if waited >= timeout {
        return 0;
    }

    let delay = BUSY_DELAYS_MS
        .get(count)
        .copied()
        .unwrap_or(BUSY_MAX_DELAY_MS)
        .min(timeout - waited);

    std::thread::sleep(Duration::from_millis(delay as u64));

    if shared.cancelled.load(Ordering::Acquire) {
        0
    } else {
        1
    }
}

fn lock(conn: &Conn) -> MutexGuard<'_, Option<Connection>> {
    conn.db
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Runs `fun` on the open connection, clearing an earlier cancel first.
fn with_db<'a>(
    env: Env<'a>,
    guard: MutexGuard<'_, Option<Connection>>,
    shared: &Shared,
    fun: impl FnOnce(&Connection) -> Reply<'a>,
) -> Term<'a> {
    match guard.as_ref() {
        None => error_term(env, atoms::closed()),
        Some(db) => {
            shared.cancelled.store(false, Ordering::Release);

            match fun(db) {
                Ok(term) | Err(term) => term,
            }
        }
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn open<'a>(env: Env<'a>, path: String, flags: i32) -> Term<'a> {
    let flags = OpenFlags::from_bits_truncate(flags)
        | OpenFlags::SQLITE_OPEN_URI
        | OpenFlags::SQLITE_OPEN_NO_MUTEX;

    let db = match Connection::open_with_flags(&path, flags) {
        Ok(db) => db,
        Err(error) => return sqlite_error(env, error),
    };

    db.set_prepared_statement_cache_capacity(STATEMENT_CACHE_CAPACITY);

    let shared = Box::new(Shared {
        cancelled: AtomicBool::new(false),
        busy_timeout_ms: AtomicI32::new(DEFAULT_BUSY_TIMEOUT_MS),
    });

    // The handler's argument is the boxed `Shared`, which outlives `db`.
    let rc = unsafe {
        ffi::sqlite3_busy_handler(
            db.handle(),
            Some(busy_handler),
            &*shared as *const Shared as *mut c_void,
        )
    };

    if rc != ffi::SQLITE_OK {
        return error_term(env, "failed to install busy handler");
    }

    let conn = Conn {
        interrupt: db.get_interrupt_handle(),
        db: Mutex::new(Some(db)),
        shared,
    };

    (atoms::ok(), ResourceArc::new(conn)).encode(env)
}

#[rustler::nif(schedule = "DirtyIo")]
fn close(env: Env, conn: ResourceArc<Conn>) -> Term {
    conn.shared.cancelled.store(true, Ordering::Release);

    match lock(&conn).take() {
        None => atoms::ok().encode(env),
        Some(db) => match db.close() {
            Ok(()) => atoms::ok().encode(env),
            Err((_db, error)) => sqlite_error(env, error),
        },
    }
}

/// Wakes a caller blocked in the busy handler and interrupts running SQL.
#[rustler::nif]
fn cancel(conn: ResourceArc<Conn>) -> rustler::Atom {
    conn.shared.cancelled.store(true, Ordering::Release);
    conn.interrupt.interrupt();
    atoms::ok()
}

#[rustler::nif]
fn set_busy_timeout(conn: ResourceArc<Conn>, timeout_ms: i32) -> rustler::Atom {
    conn.shared
        .busy_timeout_ms
        .store(timeout_ms, Ordering::Relaxed);
    atoms::ok()
}

/// Runs SQL text that may hold several statements and returns no rows.
#[rustler::nif(schedule = "DirtyIo")]
fn execute<'a>(env: Env<'a>, conn: ResourceArc<Conn>, sql: String) -> Term<'a> {
    with_db(env, lock(&conn), &conn.shared, |db| {
        db.execute_batch(&sql)
            .map(|()| atoms::ok().encode(env))
            .map_err(|error| sqlite_error(env, error))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn query<'a>(env: Env<'a>, conn: ResourceArc<Conn>, sql: Binary<'a>, params: Term<'a>) -> Term<'a> {
    with_db(env, lock(&conn), &conn.shared, |db| {
        run(env, db, sql, params)
    })
}

/// `query/3` on the calling scheduler. Returns `:contended` without running
/// anything when another caller holds the connection, so the caller can fall
/// back to `query/3` instead of blocking a normal scheduler.
#[rustler::nif]
fn query_inline<'a>(
    env: Env<'a>,
    conn: ResourceArc<Conn>,
    sql: Binary<'a>,
    params: Term<'a>,
) -> Term<'a> {
    match conn.db.try_lock() {
        Ok(guard) => with_db(env, guard, &conn.shared, |db| run(env, db, sql, params)),
        Err(TryLockError::Poisoned(poisoned)) => {
            with_db(env, poisoned.into_inner(), &conn.shared, |db| {
                run(env, db, sql, params)
            })
        }
        Err(TryLockError::WouldBlock) => atoms::contended().encode(env),
    }
}

#[rustler::nif(schedule = "DirtyIo")]
fn last_insert_rowid(env: Env, conn: ResourceArc<Conn>) -> Term {
    with_db(env, lock(&conn), &conn.shared, |db| {
        Ok((atoms::ok(), db.last_insert_rowid()).encode(env))
    })
}

#[rustler::nif(schedule = "DirtyIo")]
fn serialize(env: Env, conn: ResourceArc<Conn>) -> Term {
    with_db(env, lock(&conn), &conn.shared, |db| {
        let data = db
            .serialize(MAIN_DB)
            .map_err(|error| sqlite_error(env, error))?;

        Ok((atoms::ok(), binary(env, &data)).encode(env))
    })
}

fn run<'a>(env: Env<'a>, db: &Connection, sql: Binary<'a>, params: Term<'a>) -> Reply<'a> {
    let sql =
        std::str::from_utf8(sql.as_slice()).map_err(|_| error_term(env, "invalid SQL text"))?;
    let mut statement = db
        .prepare_cached(sql)
        .map_err(|error| sqlite_error(env, error))?;

    bind(env, &mut statement, params)?;

    let columns = statement.column_count();
    let mut rows = statement.raw_query();
    let mut out = Vec::new();

    while let Some(row) = rows.next().map_err(|error| sqlite_error(env, error))? {
        let values: Vec<Term<'a>> = (0..columns)
            .map(|index| value(env, row.get_ref_unwrap(index)))
            .collect();

        out.push(values.encode(env));
    }

    Ok((atoms::ok(), out).encode(env))
}

fn bind<'a>(
    env: Env<'a>,
    statement: &mut CachedStatement<'_>,
    params: Term<'a>,
) -> Result<(), Term<'a>> {
    let params: ListIterator<'a> = params
        .decode()
        .map_err(|_| error_term(env, "parameters must be a list"))?;

    let params: Vec<Term<'a>> = params.collect();
    let expected = statement.parameter_count();

    if params.len() != expected {
        return Err(error_term(
            env,
            format!("expected {expected} arguments, got {}", params.len()),
        ));
    }

    for (offset, param) in params.into_iter().enumerate() {
        let value = param_value(param)
            .ok_or_else(|| error_term(env, format!("unsupported parameter at {}", offset + 1)))?;

        statement
            .raw_bind_parameter(offset + 1, value)
            .map_err(|error| sqlite_error(env, error))?;
    }

    Ok(())
}

/// Integers, floats, binaries (bound as text), `nil` and `{:blob, binary}`.
fn param_value<'a>(param: Term<'a>) -> Option<ToSqlOutput<'a>> {
    let value = match param.get_type() {
        TermType::Integer => ValueRef::Integer(param.decode::<i64>().ok()?),
        TermType::Float => ValueRef::Real(param.decode::<f64>().ok()?),
        TermType::Binary => ValueRef::Text(param.decode::<Binary<'a>>().ok()?.as_slice()),
        TermType::Atom if param == atoms::nil().to_term(param.get_env()) => ValueRef::Null,
        TermType::Tuple => match rustler::types::tuple::get_tuple(param).ok()?.as_slice() {
            [tag, blob] if *tag == atoms::blob().to_term(param.get_env()) => {
                ValueRef::Blob(blob.decode::<Binary<'a>>().ok()?.as_slice())
            }
            _ => return None,
        },
        _ => return None,
    };

    Some(ToSqlOutput::Borrowed(value))
}

fn value<'a>(env: Env<'a>, value: ValueRef<'_>) -> Term<'a> {
    match value {
        ValueRef::Null => atoms::nil().encode(env),
        ValueRef::Integer(integer) => integer.encode(env),
        ValueRef::Real(real) => real.encode(env),
        ValueRef::Text(bytes) | ValueRef::Blob(bytes) => binary(env, bytes),
    }
}

fn binary<'a>(env: Env<'a>, bytes: &[u8]) -> Term<'a> {
    let mut binary = NewBinary::new(env, bytes.len());
    binary.as_mut_slice().copy_from_slice(bytes);
    Binary::from(binary).to_term(env)
}

rustler::init!("Elixir.VialKeeper.Storage.SQLite.Native");
