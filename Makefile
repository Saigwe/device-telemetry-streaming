PY ?= python
DB ?= warehouse/telemetry.duckdb
FIXTURE_DB ?= warehouse/fixture.duckdb
FLEET_DB ?= ../device-fleet-reliability/warehouse/fleet.duckdb
DAYS ?= 90
DBT = cd dbt && DUCKDB_PATH=../$(DB) dbt

.PHONY: all fixture-all pytest source source-fixture stream dbt dashboard kafka-up clean

all: pytest source stream dbt                          ## real Backblaze drive-days from project 5
fixture-all: pytest                                    ## synthetic drive-days (CI)
	$(MAKE) source-fixture stream dbt DB=$(FIXTURE_DB)
	$(PY) -m py_compile app/dashboard.py

pytest:
	$(PY) -m pytest -q tests

source:
	$(PY) scripts/prepare_source.py --db $(DB) --fleet-db $(FLEET_DB) --days $(DAYS)

source-fixture:
	$(PY) scripts/prepare_source.py --db $(DB) --fixture --days 10

stream:
	$(PY) scripts/run_stream.py --db $(DB) --source replay

dbt:
	$(DBT) build --profiles-dir .

dashboard:
	TELEMETRY_DUCKDB_PATH=$(CURDIR)/$(DB) streamlit run app/dashboard.py

kafka-up:   # needs Docker; then: run_stream.py --source kafka & produce_kafka.py
	docker compose up -d

clean:
	rm -rf warehouse dbt/target dbt/logs
