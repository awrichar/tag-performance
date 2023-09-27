#!/bin/sh
docker run --rm -p 8000:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=perf -d postgres
docker run --rm -p 8001:27017 --name mongo -d mongo
docker run --rm -p 8002:28015 --name rethinkdb -d rethinkdb
