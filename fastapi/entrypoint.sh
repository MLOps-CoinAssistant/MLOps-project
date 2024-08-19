#!/bin/bash
export ENV=prod
uvicorn app.main:app --host 0.0.0.0 --port $UVICORN_PORT
