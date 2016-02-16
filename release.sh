#!/bin/bash

set -e

#do release
mvn clean release:clean release:prepare -B -DskipITs
mvn release:perform -DskipITs