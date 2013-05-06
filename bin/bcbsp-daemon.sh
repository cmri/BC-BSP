#!/usr/bin/env bash

# BC-BSP System
# version 0.1

# bcbsp-daemon.sh

usage="Usage: bcbsp-daemon.sh [--config <conf-dir>] [--hosts hostlistfile] (start|stop) <bcbsp-command> <args...>"

# if no args specified, show usage
if [ $# -le 1 ]; then
  echo $usage
  exit 1
fi

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bcbsp-config.sh

# get arguments
startStop=$1
shift
command=$1
shift

bcbsp_rotate_log ()
{
    log=$1;
    num=5;
    if [ -n "$2" ]; then
	num=$2
    fi
    if [ -f "$log" ]; then # rotate logs
	while [ $num -gt 1 ]; do
	    prev=`expr $num - 1`
	    [ -f "$log.$prev" ] && mv "$log.$prev" "$log.$num"
	    num=$prev
	done
	mv "$log" "$log.$num";
    fi
}

if [ -f "${BCBSP_CONF_DIR}/bcbsp-env.sh" ]; then
  . "${BCBSP_CONF_DIR}/bcbsp-env.sh"
fi

# get log directory
if [ "$BCBSP_LOG_DIR" = "" ]; then
  export BCBSP_LOG_DIR="$BCBSP_HOME/logs"
fi
mkdir -p "$BCBSP_LOG_DIR"

if [ "$BCBSP_PID_DIR" = "" ]; then
  BCBSP_PID_DIR=/tmp
fi

if [ "$BCBSP_IDENT_STRING" = "" ]; then
  export BCBSP_IDENT_STRING="$USER"
fi

# some variables
export BCBSP_LOGFILE=bcbsp-$BCBSP_IDENT_STRING-$command-$HOSTNAME.log
export BCBSP_ROOT_LOGGER="INFO,DRFA"
log=$BCBSP_LOG_DIR/bcbsp-$BCBSP_IDENT_STRING-$command-$HOSTNAME.out
pid=$BCBSP_PID_DIR/bcbsp-$BCBSP_IDENT_STRING-$command.pid
# Set default scheduling priority
if [ "$BCBSP_NICENESS" = "" ]; then
    export BCBSP_NICENESS=0
fi

case $startStop in

  (start)

    mkdir -p "$BCBSP_PID_DIR"

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo $command running as process `cat $pid`.  Stop it first.
        exit 1
      fi
    fi

    if [ "$BCBSP_MASTER" != "" ]; then
      echo rsync from $BCBSP_MASTER
      rsync -a -e ssh --delete --exclude=.svn --exclude='logs/*' --exclude='contrib/hod/logs/*' $BCBSP_MASTER/ "$BCBSP_HOME"
    fi

    bcbsp_rotate_log $log
    echo starting $command, logging to $log
    cd "$BCBSP_HOME"
    nohup nice -n $BCBSP_NICENESS "$BCBSP_HOME"/bin/bcbsp --config $BCBSP_CONF_DIR $command "$@" > "$log" 2>&1 < /dev/null &
    echo $! > $pid
    sleep 1; head "$log"
    ;;
          
  (stop)

    if [ -f $pid ]; then
      if kill -0 `cat $pid` > /dev/null 2>&1; then
        echo stopping $command
        kill `cat $pid`
      else
        echo no $command to stop
      fi
    else
      echo no $command to stop
    fi
    ;;

  (*)
    echo $usage
    exit 1
    ;;

esac
