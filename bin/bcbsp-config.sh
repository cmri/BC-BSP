# BC-BSP System
# version 0.1

# bcbsp-config.sh

this="$0"
while [ -h "$this" ]; do
  ls=`ls -ld "$this"`
  link=`expr "$ls" : '.*-> \(.*\)$'`
  if expr "$link" : '.*/.*' > /dev/null; then
    this="$link"
  else
    this=`dirname "$this"`/"$link"
  fi
done

# convert relative path to absolute path
bin=`dirname "$this"`
script=`basename "$this"`
bin=`cd "$bin"; pwd`
this="$bin/$script"

# the root of the Hama installation
export BCBSP_HOME=`dirname "$this"`/..

#check to see if the conf dir is given as an optional argument
if [ $# -gt 1 ]
then
    if [ "--config" = "$1" ]
	  then
	      shift
	      confdir=$1
	      shift
	      BCBSP_CONF_DIR=$confdir
    fi
fi
 
# Allow alternate conf dir location.
BCBSP_CONF_DIR="${BCBSP_CONF_DIR:-$BCBSP_HOME/conf}"

# Source the hama-env.sh.  
# Will have JAVA_HOME and HAMA_MANAGES_ZK defined.
if [ -f "${BCBSP_CONF_DIR}/bcbsp-env.sh" ]; then
  . "${BCBSP_CONF_DIR}/bcbsp-env.sh"
fi

#check to see it is specified whether to use the grooms or the
# masters file
if [ $# -gt 1 ]
then
    if [ "--hosts" = "$1" ]
    then
        shift
        workers=$1
        shift
        export BCBSP_SLAVES="${BCBSP_CONF_DIR}/$workers"
    fi
fi
