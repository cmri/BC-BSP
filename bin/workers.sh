#!/usr/bin/env bash

# BC-BSP System
# version 0.1

# workers.sh

usage="Usage: grooms.sh [--config confdir] command..."

# if no args specified, show usage
if [ $# -le 0 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bcbsp-config.sh

# If the groomservers file is specified in the command line,
# then it takes precedence over the definition in 
# hama-env.sh. Save it here.
HOSTLIST=$BCBSP_WORKERS

if [ -f "${BCSP_CONF_DIR}/bcbsp-env.sh" ]; then
  . "${BCBSP_CONF_DIR}/bcbsp-env.sh"
fi

if [ "$HOSTLIST" = "" ]; then
  if [ "$BCBSP_WORKERS" = "" ]; then
    export HOSTLIST="${BCBSP_CONF_DIR}/workermanager"
  else
    export HOSTLIST="${BCBSP_WORKERS}"
  fi
fi

for worker in `cat "$HOSTLIST"|sed  "s/#.*$//;/^$/d"`; do
 ssh $BCBSP_SSH_OPTS $worker $"${@// /\\ }" \
   2>&1 | sed "s/^/$worker: /" &
 if [ "$BCBSP_WORKER_SLEEP" != "" ]; then
   sleep $BCBSP_WORKER_SLEEP
 fi
done

wait
