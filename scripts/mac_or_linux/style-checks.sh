#!/bin/bash

set -e

echo "Creating the Scalastyle-config.xml with  default setting checks "
sbt scalastyleGenerateConfig

echo "Running scala check"
sbt scalastyle
