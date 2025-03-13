#!/bin/bash

set -e

python3 -m nbqa ruff --ignore=E501,E402 $@ .
