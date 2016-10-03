## Eclipse 

```
mvn eclipse:clean
mvn eclipse:eclipse
```

## Package 

```
mvn clean package -DskipTests=true
```

## Setting Up Hadoop Client

```
$ wget https://archive.apache.org/dist/hadoop/core/hadoop-2.2.0/hadoop-2.2.0.tar.gz  ./
$ tar -zxvf hadoop-2.2.0.tar.gz
$ vim .bashrc
export HADOOP_HOME=/home/ablecao/software/hadoop-2.2.0
export PATH=$JAVA_HOME/bin:$JRE_HOME/bin:$CATALINA_HOME/bin:$MVN_HOME/bin:$STORM_HOME/bin:$ZOOKEEPER_HOME/bin:$HADOOP_HOME/bin:$PATH
$ source .bashrc
$ which yarn
/home/ablecao/software/hadoop-2.2.0/bin/yarn
$ which yarn
/home/ablecao/software/hadoop-2.2.0/bin/yarn
```

## Deploy JStorm Client

## Submit Topology to Yarn

```
$ storm-yarn jar -stormZip storm.zip   -topologyJar storm-start-1.0.0-SNAPSHOT.jar -topologyMainClass  storm.starter.ExclamationTopology    -topologyName   ExclamationTopology
```


## Setting up a Single Node Cluster
## Setting up the environment
```
vim ~/.bashrc
export HADOOP_HOME=~/software/hadoop-current
export HADOOP_CONF_DIR=~/software/hadoop-conf
export PATH=$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$PATH
```
## Setting up Configuration
* Setting up mapred-site.xml
```
vim $HADOOP_CONF_DIR/mapred-site.xml
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>mapreduce.cluster.temp.dir</name>
    <value>/home/caokun/cluster-data/hadoop/tmp</value>
    <final>true</final>
  </property>

  <property>
    <name>mapreduce.cluster.local.dir</name>
    <value>/home/caokun/cluster-data/hadoop/local</value>
    <description>No description</description>
    <final>true</final>
  </property>

</configuration>
```
* Setting up yarn-site.xml
```
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>yarn.resourcemanager.resource-tracker.address</name>
    <value>localhost:8031</value>
    <description>host is the hostname of the resource manager and
    port is the port on which the NodeManagers contact the Resource Manager.
    </description>
  </property>

  <property>
    <name>yarn.resourcemanager.scheduler.address</name>
    <value>localhost:8030</value>
    <description>host is the hostname of the resourcemanager and port is the port
    on which the Applications in the cluster talk to the Resource Manager.
    </description>
  </property>

  <property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler</value>
    <description>In case you do not want to use the default scheduler</description>
  </property>

  <property>
    <name>yarn.resourcemanager.address</name>
    <value>localhost:8032</value>
    <description>the host is the hostname of the ResourceManager and the port is the port on
    which the clients can talk to the Resource Manager. </description>
  </property>

  <property>
    <name>yarn.nodemanager.resource.memory-mb</name>
    <value>10240</value>
    <description>the amount of memory on the NodeManager in GB</description>
  </property>

  <property>
    <name>yarn.nodemanager.remote-app-log-dir</name>
    <value>/app-logs</value>
    <description>directory on hdfs where the application logs are moved to </description>
  </property>

  <property>
    <name>yarn.nodemanager.aux-services</name>
    <value>mapreduce_shuffle</value>
    <description>shuffle service that needs to be set for Map Reduce to run </description>
  </property>

</configuration>

```
* Setting up capacity-scheduler.xml
```
<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>

  <property>
    <name>yarn.scheduler.capacity.root.queues</name>
    <value>unfunded,default</value>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.capacity</name>
    <value>100</value>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.unfunded.capacity</name>
    <value>50</value>
  </property>

  <property>
    <name>yarn.scheduler.capacity.root.default.capacity</name>
    <value>50</value>
  </property>

</configuration>
```

## Run ResourceManager and NodeManager
```
yarn-daemon.sh start resourcemanager
yarn-daemon.sh start nodemanager

## Run randomwriter Example
```
hadoop jar hadoop-examples.jar randomwriter out
```

