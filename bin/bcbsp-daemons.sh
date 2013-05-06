#!/usr/bin/env bash

# BC-BSP System
# version 0.1

# bcbsp-daemons.sh

usage="Usage: bcbsp-daemons.sh [--config confdir] [--hosts hostlistfile] [start|stop] command args..."

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. $bin/bcbsp-config.sh

remote_cmd="cd ${BCBSP_HOME}; $bin/bcbsp-daemon.sh --config ${BCBSP_CONF_DIR} $@"
args="--config ${BCBSP_CONF_DIR} $remote_cmd"
command=$2

case $command in
  (*)
    exec "$bin/workers.sh" $args
    ;;
esac
