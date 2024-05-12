#!/bin/sh

sudo apt-get install -y postgresql-client

docker compose up -d

sleep 10
bun run init.ts