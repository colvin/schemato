# schemato testapp

An example application using `schemato` to apply schemata to a PostgreSQL
database for a [PostgREST](http://postgrest.org/) API running in Docker under
`docker-compose`.

To use, run `make` to build the Docker container for the application, then run
`make run` or `make up` to launch the application and its PostgreSQL database
under `docker-compose`. This will expose the API service on port 3000 over
HTTP.

```
# Build and deploy
make
make up

# Query for tasks.
curl -s http://localhost:3000/todo | jq .

# "Log in" to get the JWT token.
TOKEN=$( curl -s http://localhost:3000/rpc/login -H 'Accept: application/vnd.pgrst.object+json' | jq .token | tr -d '"' )

# Create a task.
curl -s http://localhost:3000/todo \
	-H "Authorization: Bearer $TOKEN" \
	-H "Content-Type: application/json" \
	-X POST \
	-d '{"task":"something"}'

# Get the task ID of the first task.
ID=$( curl -s http://localhost:3000/todo | jq .[0].id )

# Modify a task.
curl -s http://localhost:3000/todo?id=eq.$ID \
	-H "Authorization: Bearer $TOKEN" \
	-H "Content-Type: application/json" \
	-X PATCH \
	-d '{"state":"progress"}'

# Look at your task.
curl -s http://localhost:3000/todo?id=eq.$ID | jq .

# Bring it all down
make destroy
```
