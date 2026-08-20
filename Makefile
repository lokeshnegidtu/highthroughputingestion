.PHONY: up down restart build test load-test scale-workers health clean

up:
	docker compose up -d

down:
	docker compose down

restart:
	docker compose restart

build:
	docker compose build

test:
	pytest tests/ -v

load-test:
	python scripts/load_test.py --total-requests 5000 --duration 30 --concurrency 2500

scale-workers:
	docker compose up --scale worker=4 -d

health:
	python scripts/health_check.py

clean:
	docker compose down -v
	rm -rf data/storage/blobs/* data/storage/tmp/*
