#!/usr/bin/env bash

set -ex
[[ $WAITER_HOSTNAME =~ -([0-9]+)$ ]] || exit 1
ordinal=${BASH_REMATCH[1]}
echo $ordinal > waiter-instance.cnf
cat waiter-instance.cnf
