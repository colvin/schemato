#!/bin/sh
set -e

echo "PWD: $( pwd )"

# Inject runtime state such as passwords into the static files.
test -n "$POSTGREST_PASSWORD" || { echo "testapp: missing POSTGREST_PASSWORD" ; exit 1 ; }
test -n "$POSTGREST_JWT_SECRET" || { echo "testapp: missing POSTGREST_JWT_SECRET" ; exit 1 ; }
for f in *.sql postgrest.conf
do
	sed -i -e "s/POSTGREST_PASSWORD/$POSTGREST_PASSWORD/g" $f
	sed -i -e "s/POSTGREST_JWT_SECRET/$POSTGREST_JWT_SECRET/g" $f
done

schemato -h pg testapp
postgrest postgrest.conf
