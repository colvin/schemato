use std::collections::HashMap;

use clap::{App, Arg};

#[macro_use]
extern crate log;
extern crate chrono;
extern crate fern;

use postgres::params::{ConnectParams, Host};
use postgres::{Connection, TlsMode};

use glob::glob;

fn main() {
    let matches = App::new("schemato")
        .version(env!("CARGO_PKG_VERSION"))
        .author("colvin")
        .about("Container-oriented database schemata management")
        .arg(
            Arg::with_name("database")
                .value_name("DATABASE")
                .required(true)
                .help("Database name on which to operate"),
        )
        .arg(
            Arg::with_name("schemata")
                .short("s")
                .long("schemata")
                .env("SCHEMATA")
                .takes_value(true)
                .value_name("PATH")
                .default_value(".")
                .help("Path to schemata directory"),
        )
        .arg(
            Arg::with_name("host")
                .short("h")
                .long("host")
                .env("DATABASE_HOST")
                .takes_value(true)
                .value_name("ADDRESS")
                .default_value("localhost")
                .help("Database host address"),
        )
        .arg(
            Arg::with_name("port")
                .short("p")
                .long("port")
                .env("DATABASE_PORT")
                .takes_value(true)
                .value_name("PORT")
                .default_value("5432")
                .help("Database port"),
        )
        .arg(
            Arg::with_name("username")
                .short("u")
                .long("username")
                .env("DATABASE_USER")
                .takes_value(true)
                .value_name("USER")
                .default_value("postgres")
                .help("Authentication username"),
        )
        .arg(
            Arg::with_name("password")
                .short("P")
                .long("password")
                .env("DATABASE_PASS")
                .takes_value(true)
                .value_name("PASSWORD")
                .help("Authentication password"),
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
                .help("Attempt to continue through errors"),
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

    let db_name = matches.value_of("database").unwrap();
    let db_host = matches.value_of("host").unwrap();
    let db_port = must_parse_u16_arg(matches.value_of("port").unwrap(), "port");
    let db_user = matches.value_of("username").unwrap();
    let db_pass = matches.value_of("password");

    let attempts = must_parse_u32_arg(matches.value_of("attempts").unwrap(), "attempts");
    let backoff = must_parse_u64_arg(matches.value_of("backoff").unwrap(), "backoff");

    let force = matches.is_present("force");

    let prefix = matches.value_of("schemata").unwrap();
    let mut schemata: Vec<(i32, String)> = Vec::new();
    info!("loading schemata from {}", prefix);
    for g in glob(&format!("{}/[0-9][0-9][0-9][0-9].sql", prefix)).unwrap() {
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

    info!("connecting to {}@{}:{}", db_user, db_host, db_port,);
    debug!(
        "making {} attempts with a backoff of {}s",
        attempts, backoff
    );

    let anon_conn = connect_loop(db_host, db_port, db_user, db_pass, "", attempts, backoff)
        .unwrap_or_else(|| {
            error!("unable to connnect");
            std::process::exit(1);
        });

    let query_for_database = r#"
        SELECT COUNT(*) AS c
        FROM pg_catalog.pg_database
        WHERE datname = $1
    "#;

    match anon_conn.query(query_for_database, &[&db_name]) {
        Ok(rows) => {
            let c: i64 = rows.get(0).get("c");
            match c {
                0 => {
                    create_database(&anon_conn, db_name);
                }
                1 => {
                    info!("database {} exists", db_name);
                }
                _ => {
                    exit_logging_error(&format!("database {} appears {} times?", db_name, c));
                }
            }
        }
        Err(e) => {
            exit_logging_error(&format!(
                "failed to determine existence of database {}: {}",
                db_name, e
            ));
        }
    }

    info!("reconnecting to the {} database", db_name);
    let conn = connect_loop(db_host, db_port, db_user, db_pass, db_name, 1, backoff)
        .unwrap_or_else(|| {
            error!("unable to connect");
            std::process::exit(1);
        });

    let query_for_version_schema = r#"
        SELECT 1 AS has_schema
        FROM information_schema.schemata
        WHERE catalog_name = $1
        AND schema_name = $2
    "#;

    match conn.query(query_for_version_schema, &[&db_name, &"schemato"]) {
        Ok(rows) => {
            if rows.len() < 1 {
                create_schema(&conn, db_name);
            }
        }
        Err(e) => {
            exit_logging_error(&format!(
                "failed to determine existence of {}.schemato: {}",
                db_name, e
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
            apply(&conn, ver.0, prefix, &ver.1, force);
        }
    }

    conn.finish().unwrap();
    info!("complete");
}

fn exit_logging_error(err: &str) -> ! {
    error!("{}", err);
    std::process::exit(1);
}

fn must_parse_u16_arg(arg: &str, val: &str) -> u16 {
    match arg.parse::<u16>() {
        Ok(v) => {
            return v;
        }
        Err(e) => {
            exit_logging_error(&format!("bad value for {}: {}", val, e));
        }
    }
}

fn must_parse_u32_arg(arg: &str, val: &str) -> u32 {
    match arg.parse::<u32>() {
        Ok(v) => {
            return v;
        }
        Err(e) => {
            exit_logging_error(&format!("bad value for {}: {}", val, e));
        }
    }
}

fn must_parse_u64_arg(arg: &str, val: &str) -> u64 {
    match arg.parse::<u64>() {
        Ok(v) => {
            return v;
        }
        Err(e) => {
            exit_logging_error(&format!("bad value for {}: {}", val, e));
        }
    }
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

fn connect_loop(
    host: &str,
    port: u16,
    user: &str,
    pass: Option<&str>,
    db: &str,
    attempts: u32,
    backoff: u64,
) -> Option<Connection> {
    for attempt in 1..attempts + 1 {
        match connect_postgres(host, port, user, pass, db) {
            Ok(c) => {
                info!("connected on attempt {}", attempt);
                return Some(c);
            }
            Err(e) => {
                warn!("failed connection on attempt {}: {}", attempt, e);
                if attempt != attempts {
                    std::thread::sleep(std::time::Duration::from_secs(backoff));
                }
            }
        }
    }
    None
}

fn connect_postgres(
    host: &str,
    port: u16,
    user: &str,
    pass: Option<&str>,
    db: &str,
) -> Result<Connection, postgres::Error> {
    let params = ConnectParams::builder()
        .user(user, pass)
        .port(port)
        .database(db)
        .build(Host::Tcp(host.to_string()));
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
            tstamp  TIMESTAMP WITH TIME ZONE NOT NULL
        );

        CREATE OR REPLACE FUNCTION schemato.set_tstamp()
        RETURNS TRIGGER AS $$
        BEGIN
            NEW.tstamp = NOW();
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;

        CREATE TRIGGER schemato_versions_tstamp
        BEFORE INSERT ON schemato.versions
        FOR EACH ROW
        EXECUTE PROCEDURE schemato.set_tstamp();

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

fn apply(conn: &Connection, ver: i32, prefix: &str, path: &str, force: bool) {
    info!("applying version {} from {}", ver, path);
    let d = std::fs::read_to_string(format!("{}/{}", prefix, path));
    if let Err(e) = d {
        if force {
            warn!(
                "skipping version {} due to error reading {}/{}: {}",
                ver, prefix, path, e
            );
            return;
        } else {
            exit_logging_error(&format!("failed reading {}/{}: {}", prefix, path, e));
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
            if force {
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
