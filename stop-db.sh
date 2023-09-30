#!/bin/sh
docker stop postgres
docker stop mongo
docker rm postgres
docker rm mongo
