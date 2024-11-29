#!/bin/bash

astro dev stop && astro dev kill
export $(grep -v '^#' .env | xargs)
astro dev start --compose-file compose.yaml -e .env
