export JAVA_HOME=$JAVA_HOME
export STORM_ROOT_LOGGER="INFO,rollingFile"
export NIMBUS_CHILDOPTS="-Xms1g -Xmx1g -XX:ParallelGCThreads=4"
export SUPERVISOR_CHILDOPTS=" -Xms256m -Xmx256m -XX:MaxDirectMemorySize=256m -XX:ParallelGCThreads=4"
export DRPC_CHILDOPTS="-Xmx768m -XX:MaxDirectMemorySize=600m -XX:ParallelGCThreads=4"
#export STORM_EXT_CLASSPATH=