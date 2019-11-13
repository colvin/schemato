default: build


build: secrets.env .dep.testapp


secrets.env:
	install -m 600 /dev/null secrets.env
	echo "POSTGREST_PASSWORD=$$( < /dev/urandom tr -dc A-Za-z0-9 | head -c32 )" >> secrets.env
	echo "POSTGREST_JWT_SECRET=$$( < /dev/urandom tr -dc A-Za-z0-9 | head -c32 )" >> secrets.env

bin/schemato: ../src/main.rs
	cargo install --path .. --root . --force

.dep.testapp: Dockerfile bin/schemato postgrest.conf schemata/0001.sql docker-entrypoint.sh
	docker build -t schemato/testapp:latest .
	touch .dep.testapp

clean:
	rm -rf bin .crates.toml
	-docker image rm schemato/testapp:latest
	rm -rf .dep.*

up: build
	docker-compose up -d

stop:
	docker-compose stop

down:
	docker-compose down

destroy:
	docker-compose down --volumes
	rm secrets.env

logs:
	docker-compose logs -f

run: up logs


restart: down up


roll: destroy up


