/*
 * Set up the pgjwt extension.
 *
 * https://github.com/michelp/pgjwt/blob/master/pgjwt--0.1.0.sql
 * Distributed under the following license:
 *
 * The MIT License (MIT)
 *
 * Copyright (c) 2016 Michel Pelletier
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

CREATE EXTENSION pgcrypto;

CREATE OR REPLACE FUNCTION url_encode(data bytea) RETURNS text LANGUAGE sql AS $$
    SELECT translate(encode(data, 'base64'), E'+/=\n', '-_');
$$;


CREATE OR REPLACE FUNCTION url_decode(data text) RETURNS bytea LANGUAGE sql AS $$
WITH t AS (SELECT translate(data, '-_', '+/') AS trans),
     rem AS (SELECT length(t.trans) % 4 AS remainder FROM t) -- compute padding size
    SELECT decode(
        t.trans ||
        CASE WHEN rem.remainder > 0
           THEN repeat('=', (4 - rem.remainder))
           ELSE '' END,
    'base64') FROM t, rem;
$$;


CREATE OR REPLACE FUNCTION algorithm_sign(signables text, secret text, algorithm text)
RETURNS text LANGUAGE sql AS $$
WITH
  alg AS (
    SELECT CASE
      WHEN algorithm = 'HS256' THEN 'sha256'
      WHEN algorithm = 'HS384' THEN 'sha384'
      WHEN algorithm = 'HS512' THEN 'sha512'
      ELSE '' END AS id)  -- hmac throws error
SELECT url_encode(hmac(signables, secret, alg.id)) FROM alg;
$$;


CREATE OR REPLACE FUNCTION sign(payload json, secret text, algorithm text DEFAULT 'HS256')
RETURNS text LANGUAGE sql AS $$
WITH
  header AS (
    SELECT url_encode(convert_to('{"alg":"' || algorithm || '","typ":"JWT"}', 'utf8')) AS data
    ),
  payload AS (
    SELECT url_encode(convert_to(payload::text, 'utf8')) AS data
    ),
  signables AS (
    SELECT header.data || '.' || payload.data AS data FROM header, payload
    )
SELECT
    signables.data || '.' ||
    algorithm_sign(signables.data, secret, algorithm) FROM signables;
$$;


CREATE OR REPLACE FUNCTION verify(token text, secret text, algorithm text DEFAULT 'HS256')
RETURNS table(header json, payload json, valid boolean) LANGUAGE sql AS $$
  SELECT
    convert_from(url_decode(r[1]), 'utf8')::json AS header,
    convert_from(url_decode(r[2]), 'utf8')::json AS payload,
    r[3] = algorithm_sign(r[1] || '.' || r[2], secret, algorithm) AS valid
  FROM regexp_split_to_array(token, '\.') r;
$$;

/*
 * Roles and authorization
 */

CREATE ROLE api_anon nologin;
CREATE ROLE api_user nologin;

CREATE ROLE api_auth noinherit login PASSWORD 'POSTGREST_PASSWORD';

GRANT api_anon TO api_auth;
GRANT api_user TO api_auth;

CREATE SCHEMA app;

GRANT USAGE ON SCHEMA app TO api_anon;
GRANT USAGE ON SCHEMA app TO api_user;

ALTER DATABASE testapp SET "app.jwt_secret" TO 'POSTGREST_JWT_SECRET';

CREATE TYPE app.jwt_token AS (
  token text
);

-- Login
CREATE FUNCTION app.login() RETURNS app.jwt_token AS $$
  SELECT sign(
    row_to_json(r), current_setting('app.jwt_secret')
  ) AS token
  FROM (
    SELECT
      'api_user'::text as role
  ) r;
$$ LANGUAGE sql;

/*
 * TODO Application
 */

CREATE TABLE app.task_state
(
    state   VARCHAR(64) NOT NULL PRIMARY KEY,
    descr   TEXT NOT NULL
);

INSERT INTO app.task_state
(state, descr)
VALUES
('backlog', 'Task will be performed at an unknown later time'),
('progress', 'Task is underway'),
('complete', 'Task has been completed');

CREATE TABLE app.todo
(
    id              SERIAL PRIMARY KEY,
    task            TEXT NOT NULL,
    state           VARCHAR(64) NOT NULL DEFAULT 'backlog',
    ctime           TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    mtime           TIMESTAMP WITH TIME ZONE,
    FOREIGN KEY     (state)
        REFERENCES  app.task_state (state)
);

CREATE FUNCTION app.set_mtime()
RETURNS TRIGGER AS $$
BEGIN
    NEW.mtime = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER app_todo_mtime
BEFORE UPDATE ON app.todo
FOR EACH ROW
EXECUTE PROCEDURE app.set_mtime();

GRANT ALL PRIVILEGES ON app.todo TO api_user;
GRANT ALL PRIVILEGES ON SEQUENCE app.todo_id_seq TO api_user;
GRANT ALL PRIVILEGES ON app.task_state TO api_user;

GRANT SELECT ON app.todo TO api_anon;
GRANT SELECT ON SEQUENCE app.todo_id_seq TO api_anon;
GRANT SELECT ON app.task_state TO api_anon;
