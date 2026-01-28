#!/bin/bash

python -m Scripts.events_cold_start
python -m scripts.incremental_event_update
python -m Scripts.player_snapshots
python -m Scripts.player_history_dump
python -m Scripts.ingest_fixture
python -m Scripts.build_fact_table
