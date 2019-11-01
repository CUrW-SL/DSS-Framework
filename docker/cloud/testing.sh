#!/usr/bin/env bash

echo "starting test script"
./test_child.sh
return_code=$?
echo "return_code : ${return_code}"
echo "end of test script"