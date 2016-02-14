#!/bin/bash

if [[ ${TRAVIS_BRANCH} == 'master' ]]; then
    mvn test verify
else
    mvn test
fi
