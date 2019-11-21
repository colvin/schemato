use std::collections::HashMap;

use clap::{App, Arg};

#[macro_use]
extern crate log;
extern crate chrono;
extern crate fern;

use postgres::params::{ConnectParams, Host};
use postgres::{Connection, TlsMode};

use glob::glob;

const LOCK_ID: i64 = 10297114116;

struct SchematoConfig<'a> {
    db_name: &'a str,
    db_host: &'a str,
    db_port: u16,
    db_user: &'a str,
    db_pass: Option<&'a str>,
    prefix: &'a str,
    attempts: u32,
    backoff: u64,
    force: bool,
}

impl<'a> SchematoConfig<'a> {
    fn uri_safe(&self) -> String {
        return format!(
            "postgres://{}@{}:{}",
            self.db_user, self.db_host, self.db_port
        );
    }
}

fn main() {
    let matches = App::new("schemato")
        .version(env!("CARGO_PKG_VERSION"))
        .author("github.com/colvin")
        .about("database migration management for postgres-backed applications")
        .arg(
            Arg::with_name("database")
                .value_name("SCHEMATO_DATABASE")
                .required(true)
                .help("Database name on which to operate"),
        )
        .arg(
            Arg::with_name("schemata")
                .short("s")
                .long("schemata")
                .env("SCHEMATO_SCHEMATA")
                .takes_value(true)
                .value_name("PATH")
                .default_value(".")
                .help("Path to a directory containing SQL files"),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .env("SCHEMATO_DATABASE_HOST")
                .takes_value(true)
                .value_name("HOSTNAME")
                .default_value("localhost")
                .help("PostgreSQL server hostname"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .env("SCHEMATO_DATABASE_PORT")
                .takes_value(true)
                .value_name("PORT")
                .default_value("5432")
                .help("PostgreSQL server TCP port"),
        )
        .arg(
            Arg::with_name("username")
                .short("u")
                .long("username")
                .env("SCHEMATO_DATABASE_USER")
                .takes_value(true)
                .value_name("USER")
                .default_value("postgres")
                .help("Superuser username"),
        )
        .arg(
            Arg::with_name("password")
                .short("P")
                .long("password")
                .env("SCHEMATO_DATABASE_PASS")
                .takes_value(true)
                .value_name("PASSWORD")
                .help("Superuser password"),
        )
        .arg(
            Arg::with_name("attempts")
                .short("a")
                .long("attempts")
                .env("SCHEMATO_ATTEMPTS")
                .takes_value(true)
                .value_name("COUNT")
                .default_value("5")
                .help("Number of connection attempts before giving up"),
        )
        .arg(
            Arg::with_name("backoff")
                .short("b")
                .long("backoff")
                .env("SCHEMATO_BACKOFF")
                .takes_value(true)
                .value_name("SECONDS")
                .default_value("2")
                .help("Seconds to wait between connection attempts"),
        )
        .arg(
            Arg::with_name("force")
                .long("force")
                .help("Attempt to continue through some errors"),
        )
        .arg(
            Arg::with_name("quiet")
                .short("q")
                .long("quiet")
                .conflicts_with("verbose")
                .help("Suppress most output"),
        )
        .arg(
            Arg::with_name("verbose")
                .short("v")
                .long("verbose")
                .help("Print verbose information"),
        )
        .get_matches();

    let log_level = if matches.is_present("quiet") {
        log::LevelFilter::Error
    } else if matches.is_present("verbose") {
        log::LevelFilter::Debug
    } else {
        log::LevelFilter::Info
    };

    setup_logger(log_level).unwrap();

    let cfg = SchematoConfig {
        db_name: matches.value_of("database").unwrap(),
        db_host: matches.value_of("host").unwrap(),
        db_port: matches
            .value_of("port")
            .unwrap()
            .parse::<u16>()
            .unwrap_or_else(|e| exit_logging_error(&format!("Bad value for port: {}", e))),
        db_user: matches.value_of("username").unwrap(),
        db_pass: matches.value_of("password"),
        prefix: matches.value_of("schemata").unwrap(),
        attempts: matches
            .value_of("attempts")
            .unwrap()
            .parse::<u32>()
            .unwrap_or_else(|e| exit_logging_error(&format!("Bad value for attempts: {}", e))),
        backoff: matches
            .value_of("backoff")
            .unwrap()
            .parse::<u64>()
            .unwrap_or_else(|e| exit_logging_error(&format!("Bad value for backoff: {}", e))),
        force: matches.is_present("force"),
    };

    let mut schemata: Vec<(i32, String)> = Vec::new();
    info!("loading schemata from {}", cfg.prefix);
    for g in glob(&format!("{}/[0-9][0-9][0-9][0-9].sql", cfg.prefix)).unwrap() {
        match g {
            Ok(ent) => {
                let f = ent.file_name().unwrap().to_str().unwrap().to_string();
                let nv: Vec<&str> = f.split(".").take(1).collect();
                let n = nv[0].parse::<i32>().unwrap();
                schemata.push((n, f));
            }
            Err(e) => warn!("{}", e),
        }
    }

    if schemata.len() > 0 {
        schemata.sort();
    } else {
        warn!("no schemata found");
    }

    for s in &schemata {
        info!("found version {} in {}", s.0, s.1);
    }

    info!("connecting to {}", cfg.uri_safe());
    info!(
        "making {} attempts with a backoff of {}s",
        cfg.attempts, cfg.backoff
    );

    let anon_conn =
        connect_loop(&cfg, true).unwrap_or_else(|| exit_logging_error("unable to connect"));

    info!("obtaining lock");
    anon_conn
        .execute("SELECT pg_advisory_lock($1)", &[&LOCK_ID])
        .unwrap();

    let query_for_database = r#"
        SELECT COUNT(*) AS c
        FROM pg_catalog.pg_database
        WHERE datname = $1
    "#;

    match anon_conn.query(query_for_database, &[&cfg.db_name]) {
        Ok(rows) => {
            let c: i64 = rows.get(0).get("c");
            match c {
                0 => {
                    create_database(&anon_conn, cfg.db_name);
                }
                1 => {
                    info!("database {} exists", cfg.db_name);
                }
                _ => {
                    exit_logging_error(&format!("database {} appears {} times?", cfg.db_name, c));
                }
            }
        }
        Err(e) => {
            exit_logging_error(&format!(
                "failed to determine existence of database {}: {}",
                cfg.db_name, e
            ));
        }
    }

    anon_conn.finish().unwrap();

    info!("reconnecting to the {} database", cfg.db_name);
    let conn = connect_loop(&cfg, false).unwrap_or_else(|| {
        error!("unable to connect");
        std::process::exit(1);
    });

    info!("obtaining lock");
    conn.execute("SELECT pg_advisory_lock($1)", &[&LOCK_ID])
        .unwrap();

    let query_for_version_schema = r#"
        SELECT 1 AS has_schema
        FROM information_schema.schemata
        WHERE catalog_name = $1
        AND schema_name = $2
    "#;

    match conn.query(query_for_version_schema, &[&cfg.db_name, &"schemato"]) {
        Ok(rows) => {
            if rows.len() < 1 {
                create_schema(&conn, cfg.db_name);
            }
        }
        Err(e) => {
            exit_logging_error(&format!(
                "failed to determine existence of {}.schemato: {}",
                cfg.db_name, e
            ));
        }
    }

    info!("loading installed versions");

    let query_for_installed = r#"
        SELECT version
        FROM schemato.versions
        ORDER BY version ASC
    "#;

    let mut installed: HashMap<i32, bool> = HashMap::new();
    match conn.query(query_for_installed, &[]) {
        Ok(rows) => {
            for row in rows.iter() {
                let ver: i32 = row.get("version");
                installed.insert(ver, true);
            }
        }
        Err(e) => {
            exit_logging_error(&format!("failed loading installed versions: {}", e,));
        }
    }

    for ver in &schemata {
        if installed.contains_key(&ver.0) {
            info!("installed: {}", ver.0);
        } else {
            apply(&conn, ver.0, &ver.1, &cfg);
        }
    }

    conn.finish().unwrap();
    info!("complete");
}

fn exit_logging_error(err: &str) -> ! {
    error!("{}", err);
    std::process::exit(1);
}

fn setup_logger(lvl: log::LevelFilter) -> Result<(), fern::InitError> {
    fern::Dispatch::new()
        .format(|out, message, record| {
            out.finish(format_args!(
                "{} {} {:^5} -- {}",
                record.target(),
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ"),
                record.level(),
                message
            ))
        })
        .level(lvl)
        .chain(std::io::stdout())
        .apply()?;
    Ok(())
}

fn connect_loop(cfg: &SchematoConfig, anon: bool) -> Option<Connection> {
    for attempt in 1..cfg.attempts + 1 {
        match connect_postgres(cfg, anon) {
            Ok(c) => {
                info!("connected on attempt {}", attempt);
                return Some(c);
            }
            Err(e) => {
                warn!("failed connection on attempt {}: {}", attempt, e);
                if attempt != cfg.attempts {
                    std::thread::sleep(std::time::Duration::from_secs(cfg.backoff));
                }
            }
        }
    }
    None
}

fn connect_postgres(cfg: &SchematoConfig, anon: bool) -> Result<Connection, postgres::Error> {
    let params = ConnectParams::builder()
        .user(cfg.db_user, cfg.db_pass)
        .port(cfg.db_port)
        .database(if anon { "" } else { cfg.db_name })
        .build(Host::Tcp(cfg.db_host.to_string()));
    let conn = Connection::connect(params, TlsMode::None)?;
    Ok(conn)
}

fn create_database(conn: &Connection, name: &str) {
    info!("creating database {}", name);
    if let Err(e) = conn.execute(&format!("CREATE DATABASE {}", name), &[]) {
        exit_logging_error(&format!("failed creating database {}: {}", name, e));
    }
}

fn create_schema(conn: &Connection, db_name: &str) {
    info!("creating schema {}.schemato", db_name);
    let t = conn.transaction().unwrap();
    let query = r#"
        CREATE SCHEMA schemato;

        CREATE TABLE schemato.versions (
            version INTEGER NOT NULL PRIMARY KEY,
            tstamp  TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
        );

        INSERT INTO schemato.versions (version) VALUES (0);
    "#;
    if let Err(e) = t.batch_execute(query) {
        exit_logging_error(&format!(
            "failed creating schema {}.schemato: {}",
            db_name, e
        ));
    }
    t.commit().unwrap();
}

fn apply(conn: &Connection, ver: i32, path: &str, cfg: &SchematoConfig) {
    info!("applying version {} from {}", ver, path);
    let d = std::fs::read_to_string(format!("{}/{}", cfg.prefix, path));
    if let Err(e) = d {
        if cfg.force {
            warn!(
                "skipping version {} due to error reading {}/{}: {}",
                ver, cfg.prefix, path, e
            );
            return;
        } else {
            exit_logging_error(&format!("failed reading {}/{}: {}", cfg.prefix, path, e));
        }
    }
    let set_version = r#"
        INSERT INTO schemato.versions
        (version)
        VALUES
        ($1)
    "#;
    let t = conn.transaction().unwrap();
    match t.batch_execute(&d.unwrap()) {
        Ok(_) => {
            if let Err(e) = t.execute(set_version, &[&ver]) {
                exit_logging_error(&format!("failed registering version {}: {}", ver, e));
            }
        }
        Err(e) => {
            if cfg.force {
                warn!("continuing through error applying version {}: {}", ver, e);
                t.set_rollback();
                return;
            } else {
                exit_logging_error(&format!("failed applying version {}: {}", ver, e));
            }
        }
    }
    t.commit().unwrap();
}
