#!/bin/sh
# Call this script to install test dependencies

_DIR=$1

if [ -z "$_DIR" ]
then
	echo "Please pass path to directory with tarantool-enterprise as script argument"
	exit 1
fi

set -e

# Test dependencies:
tt rocks install ${_DIR}/tarantool-enterprise/rocks/errors-2.2.1-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/checks-3.3.0-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/vshard-0.1.26-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/http-1.5.0-1.linux-x86_64.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/metrics-1.0.0-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/frontend-core-8.2.2-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/luagraphqlparser-0.2.0-1.linux-x86_64.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/graphql-0.3.0-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/ddl-1.6.5-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/membership-2.4.2-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/cartridge-metrics-role-0.1.1-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/cartridge-2.8.5-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/cartridge-cli-extensions-1.1.2-1.all.rock
tt rocks install ${_DIR}/tarantool-enterprise/rocks/migrations-0.7.0-1.all.rock
cp ${_DIR}/tarantool-enterprise/tarantoolctl .
cp ${_DIR}/tarantool-enterprise/tarantool .
cp ${_DIR}/tarantool-enterprise/tt .
