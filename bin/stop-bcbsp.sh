#!/usr/bin/env bash

# BC-BSP System
# version 0.1

# stop-bcbsp.sh
# Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bcbsp-config.sh

"$bin"/bcbsp-daemons.sh --config $BCBSP_CONF_DIR stop workermanager
"$bin"/bcbsp-daemon.sh --config $BCBSP_CONF_DIR stop bspcontroller
