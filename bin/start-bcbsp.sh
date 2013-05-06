#!/usr/bin/env bash

# BC-BSP System
# version 0.1

# bcbsp-daemon.sh
# NEU softlab yijuncheng
# Run this on master node.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

. "$bin"/bcbsp-config.sh

# start bsp daem# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License atons
# start zookeeper first to minimize connection errors at startup
"$bin"/bcbsp-daemon.sh --config $BCBSP_CONF_DIR start bspcontroller
"$bin"/bcbsp-daemons.sh --config $BCBSP_CONF_DIR start workermanager
