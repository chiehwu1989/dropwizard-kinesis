#!/bin/bash

echo "${TRAVIS_TAG}"

if [[ "${TRAVIS_TAG}" == "dropwizard-kinesis-*" ]] ; then
    curl -X POST --data-urlencode "payload={ \"text\": \"${TRAVIS_TAG} has been released\"}" "${slackrelease}"
fi

mvn -B jacoco:report coveralls:report
