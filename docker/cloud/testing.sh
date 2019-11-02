#!/usr/bin/env bash

echo "starting test script"
./home/hasitha/PycharmProjects/DSS-Framework/docker/cloud/test_child.sh
return_code=$?
echo "return_code : ${return_code}"
echo "end of test script"