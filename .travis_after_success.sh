#!/bin/bash

mvn -B jacoco:report coveralls:report

version_match=`perl -e "print '$TRAVIS_TAG' =~ /^dropwizard-kinesis-\d+\.\d+\.\d+\.\d+$/"`
if [[ "$version_match" == "1" ]]; then
    mvn clean deploy --settings .travis/release-settings.xml -DskipTests=true -B
fi
