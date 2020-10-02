#!/bin/bash

set -ex

flake8 ./flonb --show-source --statistics
black ./flonb --check

flake8 ./tests --show-source --statistics
black ./tests --check
