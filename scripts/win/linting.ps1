#!/bin/bash

set -e

echo "Running checkstyle on Java source files"
./gradlew checkstyleMain

echo "Running checkstyle on the test Java source files"
./gradlew checkstyleTest
