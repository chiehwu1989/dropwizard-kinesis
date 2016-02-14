#!/bin/bash

if [[ ${TRAVIS_BRANCH} == 'master' ]]; then
    mvn test verify
else
    echo "Skipping Integration tests for branch \"${TRAVIS_BRANCH}\""
    mvn test
fi
