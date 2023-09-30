#!/bin/sh
docker run -p 8000:5432 --name postgres -e POSTGRES_HOST_AUTH_METHOD=trust -e POSTGRES_DB=perf --shm-size=512mb -d postgres
docker run -p 8001:27017 --name mongo -d mongo
