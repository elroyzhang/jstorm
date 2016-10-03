# Storm UI REST API
Storm UI server provides a REST Api to access cluster, topology, component overview and metrics. 
This api returns json response.  
REST API supports JSONP. User can pass callback query param to wrap json in the callback function.
Please ignore undocumented elements in the json repsonse.

# REST API Parameter Description
|Parameter |Value   |Description  |
|----------|--------|-------------|
|sys|true/fasle| show system component or not|
|tid|topology's id|topology's id value|
|cid|component's id|component's id value|
|sid|supervisor's id|supervisor's id value|
|tname|topology's name|topology's name value|
|wait-seconds|the delay second to operation|wait seconds|

# 如果这些值出现在参数值中，需要转换成十六进制字符，以下是对照表
|origin value |url value |
|----------|--------|
|+         |%2B     |
|空格       |+       |
|/         |%2F     |
|?         |%3F     |
|#         |%23     |
|&         |%26     |
|=         |%3D     |

## Using the UI REST Api

### /api/v1/cluster/configuration/config_key?keys1,keys2,keys3 (GET)
 returns cluster configuration.  Below is a sample response but doesn't include all the config fileds.
|Parameter |Value   |Description  |
|----------|--------|-------------|
|config_key|String (Optional)| the key of configuration  |
Sample Response:  
```json
  {
    "dev.zookeeper.path": "/tmp/dev-storm-zookeeper",
    "topology.tick.tuple.freq.secs": null,
    "topology.builtin.metrics.bucket.size.secs": 60,
    "topology.fall.back.on.java.serialization": true,
    "topology.max.error.report.per.interval": 5,
    "zmq.linger.millis": 5000,
    "topology.skip.missing.kryo.registrations": false,
    "storm.messaging.netty.client_worker_threads": 1,
    "ui.childopts": "-Xmx768m",
    "storm.zookeeper.session.timeout": 20000,
    "nimbus.reassign": true,
    "topology.trident.batch.emit.interval.millis": 500,
    "storm.messaging.netty.flush.check.interval.ms": 10,
    "nimbus.monitor.freq.secs": 10,
    "logviewer.childopts": "-Xmx128m",
    "java.library.path": "/usr/local/lib:/opt/local/lib:/usr/lib",
    "topology.executor.send.buffer.size": 1024,
    }
```
    
### /api/v1/cluster/summary (GET)
returns cluster summary such as nimbus uptime,number of supervisors,slots etc..

Response Fields:

|Field  |Value|Description
|---	|---	|---
|stormVersion|String| Storm version|
|nimbusUptime|String| Shows how long the cluster is running|
|supervisors|Integer|  Number of supervisors running|
|slotsTotal| Integer|Total number of available worker slots|
|slotsUsed| Integer| Number of worker slots used|
|slotsFree| Integer |Number of worker slots available|
|executorsTotal| Integer |Total number of executors|
|tasksTotal| Integer |Total tasks|

Sample Response:  
```
{  
   "stormVersion":"tdh1u1-SNAPSHOT-rfe0780db80748a2ac51db8d431981634f99f6067",
   "nimbusUptime":"1d22h9m50s",
   "submitTime":"2016-08-30 17:43:37",
   "supervisors":3,
   "slotsTotal":30,
   "slotsUsed":20,
   "slotsFree":10,
   "executorsTotal":60,
   "tasksTotal":71
}
```
    
### /api/v1/supervisor/summary (GET)
returns all supervisors summary 

Response Fields:

|Field  |Value|Description|
|---	|---	|---
|id| String | Supervisor's id|
|host| String| Supervisor's host name|
|uptime| String| Shows how long the supervisor is running|
|slotsTotal| Integer| Total number of available worker slots for this supervisor|
|slotsUsed| Integer| Number of worker slots used on this supervisor|

Sample Response:  
```
{  
   "supervisors":[  
      {  
         "id":"60a8c59d-98c4-44e4-9a3c-81365f1f2f74",
         "host":"10-254-89-152",
         "uptime":"1d22h14m3s",
         "uptimeSeconds":166443,
         "submitTime":"2016-08-30 17:43:40",
         "slotsTotal":10,
         "slotsUsed":7,
         "totalMem":51200.0,
         "totalCpu":2000.0,
         "usedMem":29120.0,
         "usedCpu":0.0,
         "logLink":"http://10-254-89-152:8081/daemonlog?file=supervisor.log",
         "version":"tdh1u1-SNAPSHOT"
      },
      {  
         "id":"44b0914b-c2fc-45e6-8a1e-dbd1962ef6cb",
         "host":"10-254-93-158",
         "uptime":"1d22h13m42s",
         "uptimeSeconds":166422,
         "submitTime":"2016-08-30 17:44:01",
         "slotsTotal":10,
         "slotsUsed":7,
         "totalMem":51200.0,
         "totalCpu":2000.0,
         "usedMem":29120.0,
         "usedCpu":0.0,
         "logLink":"http://10-254-93-158:8081/daemonlog?file=supervisor.log",
         "version":"tdh1u1-SNAPSHOT"
      }
   ],
   "schedulerDisplayResource":false
}
```

### /api/v1/supervisor/topologies?sid=:supervisorId&sys=:sys (GET)
returns all topologies in this supervisor

Request Fields:

|Field  |Value|Description|
|---	|---	|---
|sid| String (required)| Supervisor's id|
|sys| String(optional)| show system component or not|

Sample Response:  
```
{  
   "supervisorId":"44b0914b-c2fc-45e6-8a1e-dbd1962ef6cb",
   "slotsUsed":7,
   "slotsTotal":10,
   "host":"10-254-93-158",
   "submitTime":"2016-08-30 17:44:04",
   "uptime":"1d22h17m58s",
   "topologies":[  
      {  
         "port":16704,
         "componentId":"__acker",
         "executor":"14-14",
         "uptime":"1d4h24m9s",
         "topologyId":"bp22-3-1472614655",
         "topologyName":"bp22"
      },
      {  
         "port":16704,
         "componentId":"__eventlogger",
         "executor":"34-34",
         "uptime":"1d4h24m9s",
         "topologyId":"bp22-3-1472614655",
         "topologyName":"bp22"
      },
      {  
         "port":16704,
         "componentId":"exclaim",
         "executor":"57-58",
         "uptime":"1d4h24m9s",
         "topologyId":"bp22-3-1472614655",
         "topologyName":"bp22"
      }
   ]
}
```

### /api/v1/topology/summary (GET)
Returns all topologies summary

Response Fields:

|Field  |Value | Description|
|---	|---	|---
|id| String| Topology Id|
|encodedId| String| Topology encoded Id|
|owner| String| Machine User Name|
|name| String| Topology Name|
|status| String| Topology Status|
|uptime| String|  Shows how long the topology is running|
|submitTime| String|  Shows when topology submit|
|tasksTotal| Number |Total number of tasks for this topology|
|workersTotal| Number |Number of workers used for this topology|
|executorsTotal| Number |Number of executors used for this topology|
|replicationCount| Number |Number of topology replication|
|schedulerInfo| String |Scheduler info of topology|
|requestedMemOnHeap| Number |Topology request jvm memory on heap|
|requestedMemOffHeap| Number |Topology request jvm memory off heap|
|requestedTotalMem| Number |Topology request totoal memory(onHeap + offHeap)|
|requestedCpu| Number |Topology request Cpu|
|assignedMemOnHeap| Number |Topology assign memory on heap|
|assignedMemOffHeap| Number |Topology assign memory on heap|
|assignedTotalMem| Number |Topology assign total memory|
|assignedCpu| Number |Topology assign total cpu|

Sample Response:  
```
{  
   "topologies":[  
      {  
         "id":"bp22-3-1472614655",
         "encodedId":"bp22-3-1472614655",
         "owner":"crawler",
         "name":"bp22",
         "status":"ACTIVE",
         "uptime":"1d4h38m56s",
         "submitTime":"2016-08-31 11:37:35",
         "uptimeSeconds":103136,
         "tasksTotal":71,
         "workersTotal":20,
         "executorsTotal":60,
         "replicationCount":1,
         "schedulerInfo":null,
         "requestedMemOnHeap":0.0,
         "requestedMemOffHeap":0.0,
         "requestedTotalMem":0.0,
         "requestedCpu":0.0,
         "assignedMemOnHeap":83200.0,
         "assignedMemOffHeap":0.0,
         "assignedTotalMem":83200.0,
         "assignedCpu":0.0
      }
   ],
   "schedulerDisplayResource":false
}
```
 
### /api/v1/topology/detail?tid=:topologyId&window=:window&sys=:sys (GET)
  Returns topology information and stats. Subsititute id with topology id.

Request Parameters:
  
|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| Topology Id  |
|window    |String. Default value 0 means all-time| Window duration for metrics in seconds|
|sys       |String. Values true or false. Default value false| Controls including sys stats part of the response|


Response Fields:

|Field  |Value |Description|
|---	|---	|---
|id| String| Topology Id|
|encodedId| String| Encoded Topology Id|
|name| String |Topology Name|
|status| String |Shows Topology's current status|
|uptime| String |Shows how long the topology is running|
|tasksTotal| Integer |Total number of tasks for this topology|
|workersTotal| Integer |Number of workers used for this topology|
|executorsTotal| Integer |Number of executors used for this topology|
|window| String|  window param value defalut value is 0|
|windowHint| String | window param value in "hh mm ss" format. Default value is "All Time"|
|msgTimeout| Integer | Number of seconds a tuple has before the spout considers it failed |
|topologyStats| Array | Array of all the topology related stats per time window|
|topologyStats.windowPretty| String |Duration passed in HH:MM:SS format|
|topologyStats.window| String |User requested time window for metrics|
|topologyStats.emitted| Long |Number of messages emitted in given window|
|topologyStats.trasferred| Long |Number messages transferred in given window|
|topologyStats.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|topologyStats.acked| Long |Number of messages acked in given window|
|topologyStats.failed| Long |Number of messages failed in given window|
|spouts| Array | Array of all the spout components in the topology|
|spouts.spoutId| String |Spout id|
|spouts.encodedSpoutId| String | Encoded Spout id|
|spouts.executors| Integer |Number of executors for the spout|
|spouts.tasks| Integer |Total number of tasks for the spout|
|spouts.emitted| Long |Number of messages emitted in given window |
|spouts.transferred| Long |Total number of messages  transferred in given window|
|spouts.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|spouts.acked| Long |Number of messages acked|
|spouts.failed| Long |Number of messages failed|
|spouts.errorHost| Long |Error worker Hostname |
|spouts.errorPort| Long |Error worker port|
|spouts.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|spouts.errorLapsedSecs| Integer | Number of seconds elapsed since that last error happened in a spout|
|spouts.lastError| String |Shows the last error happened in a spout|


|bolts| Array | Array of bolt components in the topology|
|bolts.boltId| String |Bolt id|
|bolts.encodedBoltId| String |Encoded Bolt id|
|bolts.executors| Integer |Number of executor tasks in the bolt component|
|bolts.tasks| Integer |Number of instances of bolt|
|bolts.emitted| Long |Number of tuples emitted|
|bolts.transferred| Long |Total number of messages  transferred in given window|
|bolts.capacity| String (double value returned in String format) |This value indicates number of messages executed * average execute latency / time window|
|bolts.executeLatency| String (double value returned in String format) |Average time for bolt's execute method |
|bolts.executed| Long |Total number of messages  executed in given window |
|bolts.processLatency| String (double value returned in String format)  |Bolt's average time to ack a message after it's received|
|bolts.acked| Long |Number of tuples acked by the bolt|
|bolts.failed| Long |Number of tuples failed by the bolt|
|bolts.errorHost| Long |Error worker Hostname |
|bolts.errorPort| Long |Error worker port|
|bolts.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|bolts.errorLapsedSecs| Integer |Number of seconds elapsed since that last error happened in a bolt|
|bolts.lastError| String |Shows the last error occurred in the bolt|


Examples:  
```no-highlight
 1. http://ui-daemon-host-name:8080/api/v1/topology?id=WordCount3-1-1402960825
 2. http://ui-daemon-host-name:8080/api/v1/topology?id=WordCount3-1-1402960825?sys=true
 3. http://ui-daemon-host-name:8080/api/v1/topology?id=WordCount3-1-1402960825?window=600
```

Sample Response:  
```
{  
   "id":"bp22-3-1472614655",
   "encodedId":"bp22-3-1472614655",
   "name":"bp22",
   "status":"ACTIVE",
   "uptime":"1d4h50m54s",
   "submitTime":"2016-08-31 11:37:35",
   "tasksTotal":71,
   "workersTotal":20,
   "executorsTotal":60,
   "window":"600",
   "windowHint":"0d0h10m0s",
   "msgTimeout":30,
   "samplingPct":10,
   "debug":false,
   "totalBPCount":210,
   "topologyStats":[  
      {  
         "windowPretty":"All time",
         "window":0,
         "emitted":418817680520,
         "transferred":418817557220,
         "completeLatency":"13.460",
         "acked":69774291860,
         "failed":28563840
      },
      {  
         "windowPretty":"0d0h10m0s",
         "window":600,
         "emitted":2412294942,
         "transferred":2412294371,
         "completeLatency":"14.082",
         "acked":401862289,
         "failed":183312
      },
      {  
         "windowPretty":"0d3h0m0s",
         "window":10800,
         "emitted":43205894997,
         "transferred":43205882198,
         "completeLatency":"13.410",
         "acked":7192903056,
         "failed":2962310
      },
      {  
         "windowPretty":"1d0h0m0s",
         "window":86400,
         "emitted":347792966099,
         "transferred":347792863444,
         "completeLatency":"13.432",
         "acked":57934602571,
         "failed":23652186
      }
   ],
   "spouts":[  
      {  
         "spoutId":"word",
         "encodedSpoutId":"word",
         "executors":2,
         "tasks":5,
         "emitted":804087357,
         "transferred":804087337,
         "completeLatency":"14.082",
         "acked":401862289,
         "failed":183312,
         "bpCount":2
      }
   ],
   "bolts":[  
      {  
         "boltId":"__acker",
         "encodedBoltId":"__acker",
         "executors":30,
         "tasks":30,
         "emitted":402054895,
         "transferred":402054558,
         "capacity":"0.111",
         "executeLatency":"0.001",
         "executed":1206149224,
         "processLatency":"0.001",
         "acked":1206152706,
         "failed":0,
         "bpCount":175
      },
      {  
         "boltId":"exclaim",
         "encodedBoltId":"exclaim",
         "executors":4,
         "tasks":8,
         "emitted":804107255,
         "transferred":804107341,
         "capacity":"0.379",
         "executeLatency":"0.002",
         "executed":402055040,
         "processLatency":"0.002",
         "acked":402055351,
         "failed":0,
         "bpCount":6
      },
      {  
         "boltId":"updater",
         "encodedBoltId":"updater",
         "executors":4,
         "tasks":8,
         "emitted":402045255,
         "transferred":402045135,
         "capacity":"0.222",
         "executeLatency":"0.001",
         "executed":402045090,
         "processLatency":"0.001",
         "acked":402044715,
         "failed":0,
         "bpCount":27
      }
   ],
   "configuration":{  
      "storm.messaging.netty.min_wait_ms":"100",
      "topology.submitter.principal":"",
      "storm.resource.isolation.plugin":"org.apache.storm.container.cgroup.CgroupManager",
      "storm.zookeeper.auth.user":"null",
      "storm.messaging.netty.buffer_size":"5242880",
      "client.jartransformer.class":"org.apache.storm.hack.StormShadeTransformer",
      "topology.name":"bp22",
      "storm.exhibitor.port":"8080",
      "pacemaker.auth.method":"NONE",
      "ui.filter":"null",
      "worker.profiler.enabled":"false",
      "storm.id":"bp22-3-1472614655",
      "ui.http.creds.plugin":"org.apache.storm.security.auth.DefaultHttpCredentialsPlugin",
      "topology.bolts.outgoing.overflow.buffer.enable":"false",
      "supervisor.supervisors.commands":"[]",
      "logviewer.cleanup.age.mins":"10080",
      "topology.tuple.serializer":"org.apache.storm.serialization.types.ListDelegateSerializer",
      "drpc.port":"3772",
      "topology.max.spout.pending":"null",
      "topology.transfer.buffer.size":"1024",
      "logviewer.port":"8081",
      "worker.childopts":"-Xmx4096m -Xms4096m -Dsun.net.inetaddr.ttl=3 -Dsun.net.inetaddr.negative.ttl=1",
      "topology.component.cpu.pcore.percent":"10.0",
      "storm.daemon.metrics.reporter.plugins":"[\"org.apache.storm.daemon.metrics.reporters.JmxPreparableReporter\"]",
      "tdspider.jar.path":"tdspider-jar",
      "drpc.childopts":"-Xmx768m",
      "nimbus.task.launch.secs":"120",
      "logviewer.childopts":"-Xmx128m",
      "storm.zookeeper.servers":"[\"10.209.12.157\"]",
      "topology.disruptor.batch.timeout.millis":"1",
      "storm.messaging.transport":"org.apache.storm.messaging.netty.Context",
      "storm.messaging.netty.authentication":"false",
      "storm.cgroup.memory.limit.tolerance.margin.mb":"128.0",
      "storm.cgroup.hierarchy.name":"storm",
      "topology.kryo.factory":"org.apache.storm.serialization.DefaultKryoFactory",
      "worker.heap.memory.mb":"768",
      "storm.network.topography.plugin":"org.apache.storm.networktopography.DefaultRackDNSToSwitchMapping",
      "supervisor.slots.ports":"[16700,16701,16702,16703,16704,16705,16706,16707,16708,16709]",
      "nimbus.impersonation.authorizer":"org.apache.storm.security.auth.authorizer.ImpersonationAuthorizer",
      "resource.aware.scheduler.eviction.strategy":"org.apache.storm.scheduler.resource.strategies.eviction.DefaultEvictionStrategy",
      "topology.stats.sample.rate":"0.05",
      "storm.local.dir":"/data/home/crawler/cluster-data/jstorm",
      "pacemaker.host":"localhost",
      "storm.messaging.netty.max_retries":"300",
      "topology.testing.always.try.serialize":"false",
      "storm.principal.tolocal":"org.apache.storm.security.auth.DefaultPrincipalToLocal",
      "java.library.path":"/usr/local/lib:/opt/local/lib:/usr/lib",
      "worker.gc.childopts":"-XX:ParallelGCThreads=4 -XX:-UseGCOverheadLimit",
      "storm.group.mapping.service.cache.duration.secs":"120",
      "zmq.linger.millis":"5000",
      "topology.multilang.serializer":"org.apache.storm.multilang.JsonSerializer",
      "topology.kryo.register":"{}",
      "drpc.request.timeout.secs":"600",
      "zmq.threads":"1",
      "nimbus.blobstore.class":"org.apache.storm.blobstore.LocalFsBlobStore",
      "topology.state.synchronization.timeout.secs":"60",
      "topology.kryo.decorators":"[]",
      "topology.worker.shared.thread.pool.size":"4",
      "topology.submitter.user":"crawler",
      "topology.executor.receive.buffer.size":"1024",
      "supervisor.monitor.frequency.secs":"3",
      "storm.nimbus.retry.times":"5",
      "transactional.zookeeper.port":"null",
      "storm.auth.simple-white-list.users":"[]",
      "topology.scheduler.strategy":"org.apache.storm.scheduler.resource.strategies.scheduling.DefaultResourceAwareStrategy",
      "storm.zookeeper.port":"2182",
      "storm.zookeeper.retry.intervalceiling.millis":"30000",
      "storm.cluster.state.store":"org.apache.storm.cluster.ZKStateStorageFactory",
      "nimbus.thrift.port":"6627",
      "topology.disable.loadaware":"false",
      "nimbus.thrift.threads":"64",
      "supervisor.supervisors":"[]",
      "nimbus.seeds":"[\"localhost\"]",
      "storm.auth.simple-acl.admins":"[]",
      "topology.min.replication.count":"1",
      "submit.crawler.dir":"/data/home/crawler/deploy/tdspider-current/user",
      "nimbus.blobstore.expiration.secs":"600",
      "storm.group.mapping.service":"org.apache.storm.security.auth.ShellBasedGroupsMapping",
      "storm.nimbus.retry.interval.millis":"2000",
      "topology.max.task.parallelism":"null",
      "drpc.https.keystore.password":"",
      "supervisor.heartbeat.frequency.secs":"5",
      "nimbus.credential.renewers.freq.secs":"600",
      "storm.thrift.transport":"org.apache.storm.security.auth.SimpleTransportPlugin",
      "storm.cgroup.hierarchy.dir":"/cgroup/storm_resources",
      "storm.auth.simple-acl.users":"[]",
      "storm.zookeeper.auth.password":"null",
      "ui.port":"8080",
      "drpc.authorizer.acl.strict":"false",
      "topology.message.timeout.secs":"30",
      "topology.error.throttle.interval.secs":"10",
      "drpc.https.keystore.type":"JKS",
      "supervisor.memory.capacity.mb":"51200.0",
      "topology.jar.path":"topology-jar",
      "drpc.authorizer.acl.filename":"drpc-auth-acl.yaml",
      "topology.builtin.metrics.bucket.size.secs":"60",
      "storm.local.mode.zmq":"false",
      "ui.header.buffer.bytes":"4096",
      "topology.shellbolt.max.pending":"100",
      "drpc.max_buffer_size":"1048576",
      "storm.codedistributor.class":"org.apache.storm.codedistributor.LocalFileSystemCodeDistributor",
      "worker.profiler.childopts":"-XX:+UnlockCommercialFeatures -XX:+FlightRecorder",
      "nimbus.supervisor.timeout.secs":"60",
      "storm.supervisor.cgroup.rootdir":"storm",
      "topology.worker.max.heap.size.mb":"768.0",
      "storm.zookeeper.root":"/jstorm/vtdh1u1-20160801",
      "zmq.hwm":"0",
      "topology.sleep.spout.wait.strategy.time.ms":"1",
      "nimbus.topology.validator":"org.apache.storm.nimbus.DefaultTopologyValidator",
      "worker.heartbeat.frequency.secs":"1",
      "storm.messaging.netty.max_wait_ms":"1000",
      "topology.max.error.report.per.interval":"5",
      "nimbus.thrift.max_buffer_size":"1048576",
      "pacemaker.max.threads":"50",
      "supervisor.blobstore.download.max_retries":"3",
      "topology.enable.message.timeouts":"true",
      "storm.scheduler":"com.tencent.jstorm.scheduler.AbsoluteScheduler",
      "storm.messaging.netty.transfer.batch.size":"262144",
      "supervisor.run.worker.as.user":"false",
      "storm.messaging.netty.client_worker_threads":"1",
      "topology.tasks":"null",
      "storm.group.mapping.service.params":"null",
      "drpc.http.port":"3774",
      "transactional.zookeeper.root":"/jstorm/c_001/transactional",
      "supervisor.blobstore.download.thread.count":"5",
      "pacemaker.kerberos.users":"[]",
      "topology.spout.wait.strategy":"org.apache.storm.spout.SleepSpoutWaitStrategy",
      "storm.blobstore.inputstream.buffer.size.bytes":"65536",
      "supervisor.worker.timeout.secs":"30",
      "drpc.servers":"[\"localhost\"]",
      "topology.worker.receiver.thread.count":"1",
      "logviewer.max.sum.worker.logs.size.mb":"4096",
      "nimbus.file.copy.expiration.secs":"600",
      "pacemaker.port":"6699",
      "topology.worker.logwriter.childopts":"-Xmx64m",
      "drpc.http.creds.plugin":"org.apache.storm.security.auth.DefaultHttpCredentialsPlugin",
      "storm.blobstore.acl.validation.enabled":"false",
      "ui.filter.params":"null",
      "topology.workers":"20",
      "topology.environment":"null",
      "drpc.invocations.port":"3773",
      "topology.disruptor.batch.size":"100",
      "nimbus.cleanup.inbox.freq.secs":"600",
      "client.blobstore.class":"org.apache.storm.blobstore.NimbusBlobStore",
      "topology.fall.back.on.java.serialization":"true",
      "storm.nimbus.retry.intervalceiling.millis":"60000",
      "logviewer.appender.name":"A1",
      "ui.users":"null",
      "pacemaker.childopts":"-Xmx1024m",
      "storm.messaging.netty.server_worker_threads":"1",
      "scheduler.display.resource":"false",
      "storm.auth.simple-acl.users.commands":"[]",
      "ui.actions.enabled":"true",
      "storm.zookeeper.connection.timeout":"15000",
      "topology.tick.tuple.freq.secs":"null",
      "nimbus.inbox.jar.expiration.secs":"3600",
      "topology.debug":"false",
      "storm.zookeeper.retry.interval":"1000",
      "worker.log.level.reset.poll.secs":"30",
      "storm.exhibitor.poll.uripath":"/exhibitor/v1/cluster/list",
      "storm.zookeeper.retry.times":"5",
      "nimbus.code.sync.freq.secs":"120",
      "topology.component.resources.offheap.memory.mb":"0.0",
      "crawler.jobs.dir":"/data/home/crawler/deploy/tdspider-current/jobs",
      "topology.state.checkpoint.interval.ms":"1000",
      "topology.priority":"29",
      "supervisor.localizer.cleanup.interval.ms":"600000",
      "storm.health.check.dir":"healthchecks",
      "topology.disruptor.wait.timeout.millis":"1000",
      "nimbus.gain.leader.wait.secs":"1",
      "supervisor.cpu.capacity":"2000.0",
      "storm.cgroup.resources":"[\"cpu\",\"memory\"]",
      "topology.classpath":"null",
      "backpressure.disruptor.high.watermark":"0.9",
      "supervisor.localizer.cache.target.size.mb":"10240",
      "topology.worker.childopts":"null",
      "drpc.https.port":"-1",
      "topology.max.replication.wait.time.sec":"60",
      "storm.cgroup.cgexec.cmd":"/bin/cgexec",
      "topology.users":"[]",
      "topology.acker.executors":"30",
      "supervisor.worker.start.timeout.secs":"120",
      "supervisor.worker.shutdown.sleep.secs":"1",
      "logviewer.max.per.worker.logs.size.mb":"2048",
      "topology.trident.batch.emit.interval.millis":"500",
      "task.heartbeat.frequency.secs":"3",
      "supervisor.enable":"true",
      "supervisor.blobstore.class":"org.apache.storm.blobstore.NimbusBlobStore",
      "topology.backpressure.enable":"true",
      "drpc.worker.threads":"64",
      "resource.aware.scheduler.priority.strategy":"org.apache.storm.scheduler.resource.strategies.priority.DefaultSchedulingPriorityStrategy",
      "storm.messaging.netty.socket.backlog":"500",
      "nimbus.queue.size":"100000",
      "drpc.queue.size":"128",
      "topology.eventlogger.executors":"null",
      "pacemaker.base.threads":"10",
      "nimbus.childopts":"-Xmx1024m",
      "storm.zookeeper.superACL":"sasl:${nimbus-user}",
      "storm.resource.isolation.plugin.enable":"false",
      "storm.supervisor.worker.manager.plugin":"com.tencent.jstorm.daemon.supervisor.workermanager.ParallismKillWorkerManager",
      "nimbus.monitor.freq.secs":"10",
      "topology.executor.send.buffer.size":"1024",
      "transactional.zookeeper.servers":"null",
      "nimbus.task.timeout.secs":"30",
      "logs.users":"null",
      "ui.host":"0.0.0.0",
      "pacemaker.thread.timeout":"10",
      "storm.meta.serialization.delegate":"org.apache.storm.serialization.GzipThriftSerializationDelegate",
      "dev.zookeeper.path":"/tmp/dev-storm-zookeeper",
      "backpressure.disruptor.low.watermark":"0.4",
      "topology.skip.missing.kryo.registrations":"false",
      "drpc.invocations.threads":"64",
      "storm.zookeeper.session.timeout":"20000",
      "storm.workers.artifacts.dir":"workers-artifacts",
      "topology.component.resources.onheap.memory.mb":"128.0",
      "storm.log4j2.conf.dir":"log4j2",
      "storm.cluster.mode":"distributed",
      "ui.childopts":"-Xmx768m",
      "task.refresh.poll.secs":"10",
      "supervisor.childopts":"-Xmx256m",
      "task.credentials.poll.secs":"30",
      "storm.health.check.timeout.ms":"5000",
      "storm.blobstore.replication.factor":"3",
      "worker.profiler.command":"flight.bash"
   },
   "visualizationTable":[  
      {  
         ":component":"exclaim",
         ":type":"bolt",
         ":capacity":"0.37852755492125395",
         ":latency":"0.0019668984235320175",
         ":transferred":804107341,
         ":stats":[  
            {  
               ":host":"10-254-89-152",
               ":port":16704,
               ":uptimes_secs":103838,
               ":transferred":[  
                  {  
                     "10800":[  
                        {  
                           "154098912":1802587991
                        },
                        {  
                           "-1401682278":1802588081
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "0":[  
                        {  
                           "154098912":17456104740
                        },
                        {  
                           "-1401682278":17456104740
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "86400":[  
                        {  
                           "154098912":14498103959
                        },
                        {  
                           "-1401682278":14498103752
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "600":[  
                        {  
                           "154098912":100694332
                        },
                        {  
                           "-1401682278":100694364
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  }
               ]
            }
         ],
         ":link":"/component.html?component=exclaim&id=bp22-3-1472614655",
         ":inputs":[  
            {  
               ":component":"word",
               ":stream":"STRAEM_ID_WORD",
               ":sani-stream":"-1401682278",
               ":grouping":"shuffle"
            }
         ]
      },
      {  
         ":component":"word",
         ":type":"spout",
         ":capacity":"0.0",
         ":latency":"14.081847505192616",
         ":transferred":804087337,
         ":stats":[  
            {  
               ":host":"10-254-93-158",
               ":port":16706,
               ":uptimes_secs":103838,
               ":transferred":[  
                  {  
                     "10800":[  
                        {  
                           "-1401682278":3505542597
                        },
                        {  
                           "482347929":3505542702
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "0":[  
                        {  
                           "-1401682278":34088444620
                        },
                        {  
                           "482347929":34088444240
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "86400":[  
                        {  
                           "-1401682278":28295982910
                        },
                        {  
                           "482347929":28295982617
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "600":[  
                        {  
                           "-1401682278":192022439
                        },
                        {  
                           "482347929":192022459
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  }
               ]
            }
         ],
         ":link":"/component.html?component=word&id=bp22-3-1472614655",
         ":inputs":[  

         ]
      },
      {  
         ":component":"updater",
         ":type":"bolt",
         ":capacity":"0.22162919766516045",
         ":latency":"9.385502610412529E-4",
         ":transferred":402045135,
         ":stats":[             
            {  
               ":host":"10-254-89-152",
               ":port":16705,
               ":uptimes_secs":103839,
               ":transferred":[  
                  {  
                     "10800":[  
                        {  
                           "154098912":1799964929
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "0":[  
                        {  
                           "154098912":17445418060
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "86400":[  
                        {  
                           "154098912":14487636451
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  },
                  {  
                     "600":[  
                        {  
                           "154098912":100535188
                        },
                        {  
                           "-2014615088":0
                        }
                     ]
                  }
               ]
            }
         ],
         ":link":"/component.html?component=updater&id=bp22-3-1472614655",
         ":inputs":[  
            {  
               ":component":"exclaim",
               ":stream":"STRAEM_ID_WORD",
               ":sani-stream":"-1401682278",
               ":grouping":"shuffle"
            }
         ]
      }
   ]
} 
```
  

### /api/v1/topology/component/tid=:topologyId&cid=:componentId&window=0&sys=false(GET)

Returns detailed metrics and executor information

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| Topology Id  |
|cid |String (required)| Component Id |
|window    |String. Default value :all-time| window duration for metrics in seconds|
|sys       |String. Values 1 or 0. Default value 0| controls including sys stats part of the response|

Response Fields:

|Field  |Value |Description|
|---	|---	|---
|id   | String | Component's id|
|encodeId  |String (required)| Encode Component's Id  |
|name | String | Topology name|
|executors| Integer |Number of executor tasks in the component|
|tasks| Integer |Number of instances of component|
|topologyId   | String | topology's id|
|encodedTopologyId   | String |encoded topology's id|
|window   | String |window|
|componentType | String | component's type SPOUT or BOLT|
|windowHint| String | window param value in "hh mm ss" format. Default value is "All Time"|
|componentErrors| Array of Errors | List of component errors|
|componentErrors.time| Long | Timestamp when the exception occurred |
|componentErrors.errorHost| String | host name for the error|
|componentErrors.errorPort| String | port for the error|
|componentErrors.error| String |Shows the error happened in a component|
|componentErrors.errorLapsedSecs| Integer | Number of seconds elapsed since the error happened in a component |
|componentErrors.errorWorkerLogLink| String | Link to the worker log that reported the exception |
|topologyId| String | Topology's Id|
|window    |String. Default value "All Time" | window duration for metrics in seconds|
|spoutSummary or boltStats| Array |Array of component stats. **Please note this element tag can be spoutSummary or boltStats depending on the componentType**|
|spoutSummary.windowPretty| String |Duration passed in HH:MM:SS format|
|spoutSummary.window| String | window duration for metrics in seconds|
|spoutSummary.emitted| Long |Number of messages emitted in given window |
|spoutSummary.completeLatency| String (double value returned in String format) |Total latency for processing the message|
|spoutSummary.transferred| Long |Total number of messages  transferred in given window|
|spoutSummary.acked| Long |Number of messages acked|
|spoutSummary.failed| Long |Number of messages failed|
|boltStats.windowPretty| String |Duration passed in HH:MM:SS format|
|boltStats..window| String | window duration for metrics in seconds|
|boltStats.transferred| Long |Total number of messages  transferred in given window|
|boltStats.processLatency| String (double value returned in String format)  |Bolt's average time to ack a message after it's received|
|boltStats.acked| Long |Number of messages acked|
|boltStats.failed| Long |Number of messages failed|

Examples:  
```no-highlight
1. http://ui-daemon-host-name:8080/api/v1/topology/component?id=WordCount3-1-1402960825&component=spout
2. http://ui-daemon-host-name:8080/api/v1/topology/component?id=WordCount3-1-1402960825&component=spout&sys=1
3. http://ui-daemon-host-name:8080/api/v1/topology/component?id=WordCount3-1-1402960825&component=spout&window=600
```
 
Sample Response:  
```
{  
   "id":"word",
   "encodedId":"word",
   "name":"bp22",
   "executors":2,
   "tasks":5,
   "topologyId":"bp22-3-1472614655",
   "encodedTopologyId":"bp22-3-1472614655",
   "window":"10800",
   "componentType":"SPOUT",
   "windowHint":"0d3h0m0s",
   "topologyStatus":"ACTIVE",
   "debug":false,
   "samplingPct":0.0,
   "eventLogLink":"http://10-254-89-152:8081/log?file=bp22-3-1472614655%2F16705%2Fevents.log",
   "profilingAndDebuggingCapable":true,
   "profileActionEnabled":false,
   "spoutSummary":[  
      {  
         "windowPretty":"All time",
         "window":0,
         "emitted":140378486560,
         "transferred":140378477640,
         "completeLatency":"13.457",
         "acked":70160510980,
         "failed":28716500
      },
      {  
         "windowPretty":"0d0h10m0s",
         "window":600,
         "emitted":825582867,
         "transferred":825582807,
         "completeLatency":"13.067",
         "acked":412604089,
         "failed":171622
      },
      {  
         "windowPretty":"0d3h0m0s",
         "window":10800,
         "emitted":14416031033,
         "transferred":14416030258,
         "completeLatency":"13.410",
         "acked":7207105642,
         "failed":2966079
      },
      {  
         "windowPretty":"1d0h0m0s",
         "window":86400,
         "emitted":115943921785,
         "transferred":115943914319,
         "completeLatency":"13.428",
         "acked":57948283197,
         "failed":23671076
      }
   ],
   "outputStats":[  
      {  
         "stream":"STRAEM_ID_WORD",
         "emitted":7208014967,
         "transferred":7208014967,
         "completeLatency":"13.410",
         "acked":7207105642,
         "failed":2966079
      },
      {  
         "stream":"__ack_init",
         "emitted":7208015291,
         "transferred":7208015291,
         "completeLatency":"0.000",
         "acked":0,
         "failed":0
      },
      {  
         "stream":"__metrics",
         "emitted":775,
         "transferred":0,
         "completeLatency":"0.000",
         "acked":0,
         "failed":0
      }
   ],
   "executorStats":[  
      {  
         "id":"[67-69]",
         "encodedId":"%5B67-69%5D",
         "uptime":"1d4h59m59s",
         "host":"10-254-89-152",
         "port":16706,
         "emitted":7402748418,
         "transferred":7402747923,
         "completeLatency":"13.506",
         "acked":3699779112,
         "failed":1589773,
         "workerLogLink":"http://10-254-89-152:8081/log?file=bp22-3-1472614655%2F16706%2Fworker.log"
      },
      {  
         "id":"[70-71]",
         "encodedId":"%5B70-71%5D",
         "uptime":"1d5h0m0s",
         "host":"10-254-93-158",
         "port":16706,
         "emitted":7013282615,
         "transferred":7013282335,
         "completeLatency":"13.309",
         "acked":3507326530,
         "failed":1376306,
         "workerLogLink":"http://10-254-93-158:8081/log?file=bp22-3-1472614655%2F16706%2Fworker.log"
      }
   ],
   "componentErrors":[  

   ]
}
```

### /api/v1/topology/executors?tid=:topology_id&sys=:sys (GET)

get executors in topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid|String (required)| topology's id|
|sys   	   |String (optional)| show system component or not |

Response Json Example
```
{  
   "topologyId":"bp22-3-1472614655",
   "topologyName":"bp22",
   "tasksTotal":21,
   "executorsTotal":10,
   "status":"ACTIVE",
   "uptime":"1d5h4m3s",
   "executors":[  
      {  
         "host":"10-254-93-158",
         "port":16706,
         "executor":"70-71",
         "uptime":"1d5h3m49s",
         "componentId":"word",
         "supervisorId":"44b0914b-c2fc-45e6-8a1e-dbd1962ef6cb"
      },
      {  
         "host":"10-254-89-152",
         "port":16704,
         "executor":"55-56",
         "uptime":"1d5h3m47s",
         "componentId":"exclaim",
         "supervisorId":"60a8c59d-98c4-44e4-9a3c-81365f1f2f74"
      }
   ],
   "workersTotal":10
}
```
example url to GET: http://127.0.0.1:8080/api/v1/topology/executors?tid=limit1-4-1458628097

### /api/v1/topology/dag?tid=:topology_id (GET)

get dags in topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid|String (required)| topology's id|

Response Json Example
```
{"dags":[
  {"stream":"STRAEM_ID_WORD","dag":"word:1,exclaim:1,updater:1"},
  {"stream":"STRAEM_ID_COUNT","dag":"count:1,exclaim:1,sender:1"}
]}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}

```
example url to GET: http://127.0.0.1:8080/api/v1/topology/executors?tid=limit1-4-1458628097

### /api/v1/topology/status?action=activate&tname=:topology_name (PUT)

activates a  topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|action   	   |String (required and the value must be 'activate')| action of operation|
|tname   	   |String (required)| Topology name  |

```
success response body
{
  result_code: 200
  result_msg: success to activate topology :$topology_name
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to PUT: http://127.0.0.1:8080/api/v1/topology/status?action=activate&tname=$name
```
### /api/v1/topology/status?action=deactivate&tname=:topology_name (PUT)

deactivates a  topology 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|action   	   |String (required and the value must be 'deactivate')| action of operation|
|tname   	   |String (required)| Topology name  |
```
success response body
{
  result_code: 200
  result_msg: success to deactivate topology :$topology_name
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to PUT: http://127.0.0.1:8080/api/v1/topology/status?action=deactivate&tname=$name
```
### /api/v1/topology/debug?tname=:topology_name&cid=:component&debug=:debug&sampling-percentage=:sampling-percentage (POST)

let topology component send event log 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tname   	   |String (required)| Topology name  |
|cid   	   |String (optional)| component id|
|debug   	   |Boolean (true or false)| send event log or not|
|sampling-percentage   	   |Number (0-100)|how much percantage data will send event log|
```
success response body
{
  result_code: 200
  result_msg: "success setting debug parameter,  topology[&topology_name] component[&component-id] debug[&debug] sampling-percentage[&sampling-percentage]"
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to POST: http://127.0.0.1:8080/api/v1/topology/debug?tname=limit0&cid=word&debug=true&sampling-percentage=30
```
### (POST) /api/v1/topology/profiler?tid=:topologyId&host=:hostname&port=:port1&profile-action=:profile-action

let topology component send event log 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| Topology id  |
|host   	   |String (hostname)| hostname|
|port   	   |String (port)|one port|
|profile-action   	   |Number (0-5)|0: jprofile stop, 1: jprofile start, 2: jprofile dump, 3: jmap dump, 4: jstack dump, 5: jvm restart|
```
success response body
{
  result_code: 200
  result_msg: "success set profile action,  topologyId[&topology-id] host[&hostname] profileAction[&profile-action]  port[&port1,port2...]"
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to POST: http://127.0.0.1:8080/api/v1/topology/profiler?tid=limit0-1-1464659491&host=localhost&port=6700&profile-action=3
```

### (GET) /api/v1/topology/logconfig?tid=:topologyId

let topology component send event log 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| Topology id  |

```
success response body
{
    "namedLoggerLevels": {
        "ROOT": {
            "reset_level": null, 
            "timeout_epoch": "1465800168", 
            "target_level": "DEBUG", 
            "timeout": "50"
        }
    }
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to GET: http://127.0.0.1:8080/api/v1/topology/logconfig?tid=abcd-1-1465786065
```

### (POST) /api/v1/topology/logconfig?tid=:topologyId data="{"namedLoggerLevels":{$name:{"target_level":$target_level,"reset_level":$reset_level,"timeout":$timeout}}}"

let topology component send event log 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| Topology id  |
|data      |json String (required)| logconfig info|

```
success response body
{
    "namedLoggerLevels": {
        "ROOT": {
            "reset_level": null, 
            "timeout_epoch": "1465800168", 
            "target_level": "DEBUG", 
            "timeout": "50"
        }
    }
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to GET: http://127.0.0.1:8080/api/v1/topology/logconfig?tid=:topologyId data="{"namedLoggerLevels":{"ROOT":{"target_level":"DEBUG","reset_level":"INFO","timeout":50}}}"
```
###  api/v1/topology (GET)

list all topologies
```
sample response body
{"topologies":
    [
        {"topology_name":"limit2","status":"ACTIVE","num_tasks":"2","num_workers":"1","uptime_secs":"5869"},
        {"topology_name":"limit1","status":"ACTIVE","num_tasks":"6","num_workers":"3","uptime_secs":"5875"}
    ]
}
```

###  api/v1/topology?tname=:topology_name&wait-seconds=:wait-seconds&num-workers=:num-workers&executors=:executors (PUT)

rebalances a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tname   	   |String (required)| Topology Name  |
|wait-seconds |String (required)| Wait time before rebalance happens |
|num-workers |String (required)| the number of workers to rebalance |
|executors |String (required)| component to numtasks, like :componedname=2,componedname2=3 |

```
success response body
{
  result_code: 200
  result_msg: success to rebalance topology :$topology_name
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to PUT: http://127.0.0.1:8080/api/v1/topology?tname=limit0&wait-seconds=0&num-workers=2&executors=
```
### api/v1/topology?tname=tname1,tname2&wait-seconds=:wait-seconds (DELETE)

kills a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tname   	   |String (required)| Topology Id, many value split by ','  |
|wait-seconds |String (required)| Wait time before kill happens |

```
success response body
{
  result_code: 200
  result_msg: success to kill topology :$topology_name
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to DELETE: http://127.0.0.1:8080/api/v1/topology?tname=limit0&wait-seconds=0
```
### /api/v1/topology/jar?jar-type=tpoology.jar&jar-name=:jar-name&param-file-name=:param-file-name&FormData=file (POST)

upload a topology jar

|Parameter |Value   |Description  |
|----------|--------|-------------|
|jar-type   	   |String (required)| set default value: topology.jar|
|jar-name   	   |String (required)| the name of upload  jar|
|param-file-name   	   |String (optional)| the name of param file, if it exists means upload param file of jar|

|FormData |File (required)| the jar file of topology |
```
success response body
{
  result_code: 200
  result_msg: success upload jar!
  {jar-name: $jar-id} or {param-file-name: $param-file-name}
}
```
```
fail response body 
{  
  result_code: 500l
  result_msg: error_msg
}
```
### /api/v1/topology/jar?jar-type=tpoology.jar&jar-name=:jar-name (DELETE)

delete a topology jar

|Parameter |Value   |Description  |
|----------|--------|-------------|
|jar-type   	   |String (required)| set default value: topology.jar|
|jar-name   	   |String (required)| the name of jar file|
```
success response body
{
  result_code: 200
  result_msg: success delete jar : $jarName !
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
### /api/v1/topology/jar?jar-type=tpoology.jar (GET)

list all topology jar in topology-jar dir

|Parameter |Value   |Description  |
|----------|--------|-------------|
|jar-type   	   |String (required)| set default value: topology.jar|

```
success response body
{
  "jar-files":
    [
      {"file-name":"topology-example-1.0-SNAPSHOT.jar","upload-time":"2016-04-18 10:32"},
      {"file-name":"storm-core-0.9.4.jar","upload-time":"2016-04-18 10:36"}
    ]
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
### /api/v1/topology?jar-type=tpoology.jar&jar-name=:jar-name&main-class=:main-class&args=:args0,args1,args2...&storm-options=:option1,option2... (POST)

submit a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|jar-type   	   |String (required)| set default value: topology.jar|
|jar-name   	   |String (required)| topoology jar name in topology-jar dir  |
|main-class |String (required)| topology main class name |
|args |String (required)| topology parameters assemble by ',', example: args=a,b,c |
|storm-options |String (optional)| storm options assemble by ',', example: topology.worker.childopts="-Xmx3333m",topology.worker.gc.childopts="-XX:ParallelGCThreads=11"  |
```
success response body
{
  result_code: 200
  result_msg: success to submit topology
}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
```
example url to POST: http://127.0.0.1:8080/api/v1/topology?jar_-path=/home/yuzhongliu/deploy/storm-current/examples/topology-example/topology-example-1.0.3-SNAPSHOT.jar&main-class=com.tencent.example.LimitCountTopology&tname=limit0&args=200,0
```

### /api/v1/topology/visualization-init?tid=:tid (GET)

visualization a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| topoology's id|
```
success response body
{"visualizationTable":[{
  ":row":[{
     ":stream":"STRAEM_ID_COUNT",":sani-stream":"-520944801",":checked":"false"},
    {":stream":"STRAEM_ID_WORD",":sani-stream":"-1401682278",":checked":"false"}
  ]}
]}
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
### /api/v1/topology/visualization?tid=:tid (GET)

visualization a topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|tid   	   |String (required)| topoology's id|
```
success response body
[
  {":component":"sender",":type":"bolt",":capacity":"0.07456310679611651",":latency":"0.054",":transferred":20000,
   ":stats":[{":host":"localhost",":port":6700,":uptimes_secs":103,
    ":transferred":[{"10800":[{"154098912":20000}]},{"0":[{"154098912":20000}]},{"86400":[{"154098912":20000}]},{"600":[{"154098912":20000}]}]}],
   ":inputs":[{":component":"exclaim",":stream":"STRAEM_ID_COUNT",":sani-stream":"-520944801",":grouping":"shuffle"}]},
  {":component":"count",":type":"spout",":capacity":"0.0",":latency":"161.414",":transferred":40020,
   ":stats":[{":host":"localhost",":port":6700,":uptimes_secs":103,
    ":transferred":[{"10800":[{"482347929":20020},{"-520944801":20000}]},{"0":[{"482347929":20020},{"-520944801":20000}]},
	 {"86400":[{"482347929":20020},{"-520944801":20000}]},{"600":[{"482347929":20020},{"-520944801":20000}]}]}],
   ":inputs":[]},
  {":component":"exclaim",":type":"bolt",":capacity":"0.1803921568627451",":latency":"0.33731496342933015",":transferred":120020,
   ":stats":[{":host":"localhost",":port":6702,":uptimes_secs":102,
    ":transferred":[{"10800":[{"154098912":21480},{"-1401682278":10700},{"25125138":21620},{"-520944801":10860}]},
     {"0":[{"154098912":21480},{"-1401682278":10700},{"25125138":21620},{"-520944801":10860}]},
     {"86400":[{"154098912":21480},{"-1401682278":10700},{"25125138":21620},{"-520944801":10860}]},
     {"600":[{"154098912":21480},{"-1401682278":10700},{"25125138":21620},{"-520944801":10860}]}]},
     {":host":"localhost",":port":6701,":uptimes_secs":102,
    ":transferred":[{"10800":[{"154098912":18500},{"-1401682278":9060},{"25125138":18400},{"-520944801":9400}]},
     {"0":[{"154098912":18500},{"-1401682278":9060},{"25125138":18400},{"-520944801":9400}]},
     {"86400":[{"154098912":18500},{"-1401682278":9060},{"25125138":18400},{"-520944801":9400}]},
     {"600":[{"154098912":18500},{"-1401682278":9060},{"25125138":18400},{"-520944801":9400}]}]}],
   ":inputs":[
    {":component":"count",":stream":"STRAEM_ID_COUNT",":sani-stream":"-520944801",":grouping":"shuffle"},
    {":component":"word",":stream":"STRAEM_ID_WORD",":sani-stream":"-1401682278",":grouping":"shuffle"}]},
  {":component":"word",":type":"spout",":capacity":"0.0",":latency":"350.632",":transferred":40000,
   ":stats":[{":host":"localhost",":port":6702,":uptimes_secs":102,
    ":transferred":[{"10800":[{"-1401682278":20000},{"482347929":20000}]},
     {"0":[{"-1401682278":20000},{"482347929":20000}]},
     {"86400":[{"-1401682278":20000},{"482347929":20000}]},
     {"600":[{"-1401682278":20000},{"482347929":20000}]}]}],
   ":inputs":[]},
 {":component":"updater",":type":"bolt",":capacity":"0.19254901960784312",":latency":"0.225",":transferred":20000,
  ":stats":[{":host":"localhost",":port":6701,":uptimes_secs":102,
   ":transferred":[{"10800":[{"154098912":20000}]},
    {"0":[{"154098912":20000}]},
    {"86400":[{"154098912":20000}]},
    {"600":[{"154098912":20000}]}]}],
  ":inputs":[{":component":"exclaim",":stream":"STRAEM_ID_WORD",":sani-stream":"-1401682278",":grouping":"shuffle"}]}
]
```
```
fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```

### /api/v1/drpc/result?func=:func_name&param=:param (GET)

get result from drcp topology

|Parameter |Value   |Description  |
|----------|--------|-------------|
|func   	   |String (required)| the func name of drpc topology |
|param   	   |String (required)| the request param to drpc topology |

```
success response body
{
$drpc result
}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```

### (POST) /api/v1/crawler/conf?site.id=:siteId&site.type=:siteType&crawl.job.class=:jobClass&crawl.job.childopts=:childopts&filter.regex.list=:filterList&match.regex.list=:matchList&seeds=:seeds&number.of.crawlers=:crawlers-number&crawl.job.schedule.frequence=:schedule-frequence&cookies.ignore=:cookies-ignore&cookies.load.file=:cookies-load-file&cookies.domain=:cookies-domain&politeness.delay.max=:politeness-delay-max&politeness.delay.min=:politeness-delay-min&robots.is.obey=:robots-is-obey&tdspider.debug=:tdspider-debug

create a crawler job file

|Parameter |Value   |Description  |
|----------|--------|-------------|
|site.id   	       |String (optional)| site.id|
|site.type   	   |String (optional)| site.type|
|crawl.job.class   	   |String (optional)| crawl.job.class|
|crawl.job.childopts   	   |String (optional)| crawl.job.childopts|
|filter.regex.list  	   |String (optional)| filter.regex.list many values split by ','|
|match.regex.list  	   |String (optional)| match.regex.list many values split by ','|
|seeds  	   |String (optional)| seedst many values split by ','|
|number.of.crawlers  	   |Integer (optional)|number.of.crawlers|
|crawl.job.schedule.frequence  	   |Integer (optional)|crawl.job.schedule.frequence|
|cookies.ignore  	   |Boolean (optional)|cookies.ignore|
|cookies.load.file  	   |String (optional)|cookies.load.file|
|cookies.domain  	   |String (optional)|cookies.domain|
|politeness.delay.max  	   |Integer (optional)|politeness.delay.max|
|politeness.delay.min  	   |Integer (optional)|politeness.delay.min|
|robots.is.obey  	   |Boolean (optional)|robots.is.obey|
|tdspider.debug  	   |Boolean (optional)|tdspider.debug|

```
success response body 
{  
  result_code: 200
  result_msg: success config job file! 
}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```

### (PUT) /api/v1/crawler/conf?site.id=:siteId&site.type=:siteType&crawl.job.class=:jobClass&crawl.job.childopts=:childopts&filter.regex.list=:filterList&match.regex.list=:matchList&seeds=:seeds&number.of.crawlers=:crawlers-number&crawl.job.schedule.frequence=:schedule-frequence&cookies.ignore=:cookies-ignore&cookies.load.file=:cookies-load-file&cookies.domain=:cookies-domain&politeness.delay.max=:politeness-delay-max&politeness.delay.min=:politeness-delay-min&robots.is.obey=:robots-is-obey&tdspider.debug=:tdspider-debug

update a crawler job file

|Parameter |Value   |Description  |
|----------|--------|-------------|
|site.id   	       |String (optional)| site.id|
|site.type   	   |String (optional)| site.type|
|crawl.job.class   	   |String (optional)| crawl.job.class|
|crawl.job.childopts   	   |String (optional)| crawl.job.childopts|
|filter.regex.list  	   |String (optional)| filter.regex.list many values split by ','|
|match.regex.list  	   |String (optional)| match.regex.list many values split by ','|
|seeds  	   |String (optional)| seedst many values split by ','|
|number.of.crawlers  	   |Integer (optional)|number.of.crawlers|
|crawl.job.schedule.frequence  	   |Integer (optional)|crawl.job.schedule.frequence|
|cookies.ignore  	   |Boolean (optional)|cookies.ignore|
|cookies.load.file  	   |String (optional)|cookies.load.file|
|cookies.domain  	   |String (optional)|cookies.domain|
|politeness.delay.max  	   |Integer (optional)|politeness.delay.max|
|politeness.delay.min  	   |Integer (optional)|politeness.delay.min|
|robots.is.obey  	   |Boolean (optional)|robots.is.obey|
|tdspider.debug  	   |Boolean (optional)|tdspider.debug|


如果这些值出现在参数值中，需要转换成十六进制字符，以下是对照表

|origin value |url value |
|----------|--------|
|+         |%2B     |
|空格       |+       |
|/         |%2F     |
|?         |%3F     |
|#         |%23     |
|&         |%26     |
|=         |%3D     |


```
success response body 
{  
  result_code: 200
  result_msg: success update job file! 
}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```
### (DELETE) /api/v1/crawler/conf?site.id=:siteid

delete a crawler job file

|Parameter |Value   |Description  |
|----------|--------|-------------|
|site.id   	   |String (required)| job's site id|

```
success response body 
{  
  result_code: 200
  result_msg: success delete job file! 
}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```

### (GET) /api/v1/crawler/conf?site.id=:site.id

get a crawler job file

|Parameter |Value   |Description  |
|----------|--------|-------------|
|site.id   	   |String (required)| job's site id|

```
success response body 
{"job-confs":
 [
  "filter.regex.list":
   [".*(?i)(\\.(js|css))",".*(?i)(\\.(avi|wmv|mpe?g|mp3|swf))",".*(?i)(\\.(rar|zip|tar|gz))",".*(?i)(\\.(bmp|gif|jpe?g|png|ico|svg|tiff?)).*",
    ".*(?i)(\\.(pdf|doc|xls|odt|txt|conf|pdf))",".*hr.weibo.com.*",".*help.weibo.com.*",".*radio.weibo.com.*",".*gouwu.weibo.com.*",
    ".*photo.weibo.com.*",".*music.weibo.com.*",".*movie.weibo.com.*",".*ting.weibo.com.*",".*vote.weibo.com.*",".*screen.weibo.com.*",
    ".*manhua.weibo.com.*",".*book.weibo.com.*",".*caipiao.weibo.com.*",".*vdisk.weibo.com.*",".*game.weibo.com.*",".*open.weibo.com.*",
    ".*ir.weibo.com.*",".*app.weibo.com.*",".*apps.weibo.com.*",".*data.weibo.com.*",".*talk.weibo.com.*",".*vgirl.weibo.com.*",
    ".*food.weibo.com.*",".*gongyi.weibo.com.*",".*passport.weibo.com.*",".*verified.weibo.com.*",".*tui.weibo.com.*"],
  "match.regex.list":[".*weibo\\.com/\\d+/[a-zA-Z0-9]+(\\S*).*"],
  "crawl.job.class":"com.tencent.tdspider.rules.WeiBoRule",
  "site.type":2,
  "cookies.load.file":"weibo_cookies.txt",
  "seeds":
   ["http://weibo.com/1784473157/D4xZv5Mtz","http://weibo.com/u/2505217870/home?wvr=5","http://weibo.com/breakingnews",
    "http://weibo.com/shmorningpost","http://weibo.com/chinanewsweek","http://weibo.com/chinanewsv",
    "http://weibo.com/cctvxinwen","http://weibo.com/","http://d.weibo.com/","http://d.weibo.com/102803",
    "http://d.weibo.com/102803?topnav=1&mod=logo&wvr=6","http://d.weibo.com/100803?topnav=1&mod=logo&wvr=6",
    "http://d.weibo.com/102803_ctg1_4688_-_ctg1_4688","http://d.weibo.com/102803_ctg1_4188_-_ctg1_4188",
    "http://d.weibo.com/102803_ctg1_2088_-_ctg1_2088","http://d.weibo.com/102803_ctg1_6288_-_ctg1_6288",
    "http://d.weibo.com/102803_ctg1_5988_-_ctg1_5988","http://d.weibo.com/102803_ctg1_5088_-_ctg1_5088",
    "http://d.weibo.com/102803_ctg1_6388_-_ctg1_6388","http://d.weibo.com/102803_ctg1_1288_-_ctg1_1288",
    "http://d.weibo.com/102803_ctg1_4288_-_ctg1_4288","http://d.weibo.com/102803_ctg1_4688_-_ctg1_4688",
    "http://d.weibo.com/102803_ctg1_2488_-_ctg1_2488","http://d.weibo.com/102803_ctg1_3288_-_ctg1_3288",
    "http://d.weibo.com/102803_ctg1_5288_-_ctg1_5288","http://d.weibo.com/102803_ctg1_5188_-_ctg1_5188",
    "http://d.weibo.com/102803_ctg1_1388_-_ctg1_1388","http://d.weibo.com/102803_ctg1_4788_-_ctg1_4788",
    "http://d.weibo.com/102803_ctg1_2188_-_ctg1_2188","http://d.weibo.com/102803_ctg1_6488_-_ctg1_6488",
    "http://d.weibo.com/102803_ctg1_6588_-_ctg1_6588","http://d.weibo.com/102803_ctg1_6688_-_ctg1_6688",
    "http://d.weibo.com/102803_ctg1_6788_-_ctg1_6788","http://d.weibo.com/102803_ctg1_2288_-_ctg1_228",
    "http://d.weibo.com/102803_ctg1_1988_-_ctg1_1988","http://d.weibo.com/102803_ctg1_6888_-_ctg1_6888",
    "http://d.weibo.com/102803_ctg1_4388_-_ctg1_4388","http://d.weibo.com/102803_ctg1_7088_-_ctg1_7088",
    "http://d.weibo.com/102803_ctg1_5788_-_ctg1_5788","http://d.weibo.com/102803_ctg1_1688_-_ctg1_1688",
    "http://d.weibo.com/102803_ctg1_4588_-_ctg1_4588","http://d.weibo.com/102803_ctg1_6399_-_ctg1_6399"],
  "robots.is.obey":false,
  "cookies.domain":".weibo.com",
  "site.id":3,
  "is.follow.redirects":true,
  "politeness.delay.max":1000,
  "cookies.ignore":false,
  "crawl.job.childopts":"-Xmx1024m",
  "politeness.delay.min":500
 },
 {
  "filter.regex.list":
   [".*(\\.(css|js|gif|jpg|png|mp3|mp3|zip|gz))$",".*36kr.com/users.*",".*passport.36kr.com.*",".*36kr.com/pages/hire.*",
    ".*download.36kr.com.*",".*next.36kr.com.*",".*events.36kr.com"],
  "match.regex.list":[".*36kr.com/p/\\d+.html.*"],
  "crawl.job.childopts":"-Xmx1024m",
  "crawl.job.class":"com.tencent.tdspider.rules.Com36krRule",
  "site.type":1,
  "seeds":["http://36kr.com"],
  "site.id":4
  }
 ]
}

fail response body 
{  
  result_code: 500
  result_msg: error_msg
}
```

### (POST) /api/v1/crawler/jobs?site.id=:siteid1,:siteid2,:siteid3&main-class=:main-class&num-workers=:num-workers&num-frontier=:num-frontier&num-fetcher-parser=:num-fetcher-parser&crawler-file=:crawler-file

submit crawler job topology by crawler job file

|Parameter |Value   |Description  |
|----------|--------|-------------|
|site.id   	   |String (required)| job's site id, many values split by ',', and 'ALL' represent submit all the job file in jobs dir|
|main-class   	   |String (optional)| job's crawler main class, if not given ,default value is 'com.tencent.jstorm.crawler.CrawlTopology' |
|num-workers   	   |Integer (optional)| topology's workers number, if not given ,default value is 1|
|num-frontier   	   |Integer (optional)| cralwer's frontier number, if not given ,default value is 1|
|num-fetcher-parser   	   |Integer (optional)| cralwer's fetcher-parser number, if not given ,default value is 1|
|crawler-file   	   |String (optional)| cralwer's common config file, if not given ,default value is 'crawler.yaml'|


```
success response body 
{
  "result_code":"200",
  "result_msg":"success submit job: [job-id: 8 topology-name: crawl-job-8, job-id: 9 topology-name: crawl-job-9]"
}

fail response body 
{  
  result_code: 500,
  result_msg: error_msg
}
```


### (GET) /api/v1/crawler/jobs-summary

get all crawler job summary 

Response Fields:

|Field  |Value|Description
|---	|---	|---
|site-id|String| site.id|
|topology-id|String| the topology id of site|
|topology-name|String|the topology name of site|
```
success response body 
{
 "job-summarys":
  [
   {"topology-id":"", "site-id":"1", "topology-name":""},
   {"topology-id":"", "site-id":"2", "topology-name":""},
   {"topology-id":"crawl-job-3-8-1460448841","site-id":"3","topology-name":"crawl-job-3"},
   {"topology-id":"crawl-job-4-9-1460448841","site-id":"4","topology-name":"crawl-job-4"},
   {"topology-id":"", "site-id":"5", "topology-name":""},
   {"topology-id":"", "site-id":"6", "topology-name":""}
  ]
}

fail response body 
{  
  result_code: 500,
  result_msg: error_msg
}
```
### (GET) /api/v1/topology/workers?topology_id=$topology_id

get worker jvm info 

|Parameter |Value   |Description  |
|----------|--------|-------------|
|topology_id  |String (optional)|topology id|
Response Fields:

```
success response body 
[
    {
        "topologyId": "abcd-4-1466054777", 
        "component": "[word, com.tencent.jstorm.metric.SystemHealthMetricsConsumer, __acker]", 
        "host": "localhost", 
        "port": 6701, 
        "workerLogLink": "http://localhost:8000/logs/workers-artifacts%2Fabcd-4-1466054777%2F6701%2Fworker.log", 
        "uptimeSecs": "0d0h46m14s", 
        "startTime": "2016-06-16 13:26:38", 
        "processInfo": {
            "processMemeoryUsed": "9.2%", 
            "processPhysicalMemoryUsed": "0.36 GB", 
            "processCPUUsed": "20%"
        }, 
        "memory/nonHeap": {
            "unusedMB": "0", 
            "virtualFreeMB": "-42", 
            "initMB": "2", 
            "committedMB": "43", 
            "maxMB": "-1", 
            "usedMB": "42"
        }, 
        "jvmTime": { }, 
        "GC/PSScavenge": {
            "count": "0", 
            "timeMs": "0"
        }, 
        "GC/PSMarkSweep": {
            "count": "0", 
            "timeMs": "0"
        }, 
        "memory/heap": {
            "unusedMB": "936", 
            "virtualFreeMB": "936", 
            "initMB": "1024", 
            "committedMB": "1010", 
            "maxMB": "1010", 
            "usedMB": "73"
        }, 
        "workerInfo": {
            "host": "localhost", 
            "componentId": "__system", 
            "port": "6701", 
            "taskId": "-1"
        }
    }, 
    {
        "topologyId": "abcd-4-1466054777", 
        "component": "[updater, __eventlogger, __acker]", 
        "host": "localhost", 
        "port": 6700, 
        "workerLogLink": "http://localhost:8000/logs/workers-artifacts%2Fabcd-4-1466054777%2F6700%2Fworker.log", 
        "uptimeSecs": "0d0h46m12s", 
        "startTime": "2016-06-16 13:26:40", 
        "processInfo": {
            "processMemeoryUsed": "9.6%", 
            "processPhysicalMemoryUsed": "0.37 GB", 
            "processCPUUsed": "14%"
        }, 
        "memory/nonHeap": {
            "unusedMB": "0", 
            "virtualFreeMB": "-42", 
            "initMB": "2", 
            "committedMB": "43", 
            "maxMB": "-1", 
            "usedMB": "42"
        }, 
        "jvmTime": { }, 
        "GC/PSScavenge": {
            "count": "1", 
            "timeMs": "83"
        }, 
        "GC/PSMarkSweep": {
            "count": "0", 
            "timeMs": "0"
        }, 
        "memory/heap": {
            "unusedMB": "969", 
            "virtualFreeMB": "969", 
            "initMB": "1024", 
            "committedMB": "1010", 
            "maxMB": "1010", 
            "usedMB": "40"
        }, 
        "workerInfo": {
            "host": "localhost", 
            "componentId": "__system", 
            "port": "6700", 
            "taskId": "-1"
        }
    }
]

fail response body 
{  
  result_code: 500,
  result_msg: error_msg
}
```

### ERRORS
on errors in any of the above api returns 500 http status code with 
the following response.

Sample Response:  
```json
{
  "error": "Internal Server Error",
  "errorMessage": "java.lang.NullPointerException\n\tat clojure.core$name.invoke(core.clj:1505)\n\tat backtype.storm.ui.core$component_page.invoke(core.clj:752)\n\tat backtype.storm.ui.core$fn__7766.invoke(core.clj:782)\n\tat compojure.core$make_route$fn__5755.invoke(core.clj:93)\n\tat compojure.core$if_route$fn__5743.invoke(core.clj:39)\n\tat compojure.core$if_method$fn__5736.invoke(core.clj:24)\n\tat compojure.core$routing$fn__5761.invoke(core.clj:106)\n\tat clojure.core$some.invoke(core.clj:2443)\n\tat compojure.core$routing.doInvoke(core.clj:106)\n\tat clojure.lang.RestFn.applyTo(RestFn.java:139)\n\tat clojure.core$apply.invoke(core.clj:619)\n\tat compojure.core$routes$fn__5765.invoke(core.clj:111)\n\tat ring.middleware.reload$wrap_reload$fn__6880.invoke(reload.clj:14)\n\tat backtype.storm.ui.core$catch_errors$fn__7800.invoke(core.clj:836)\n\tat ring.middleware.keyword_params$wrap_keyword_params$fn__6319.invoke(keyword_params.clj:27)\n\tat ring.middleware.nested_params$wrap_nested_params$fn__6358.invoke(nested_params.clj:65)\n\tat ring.middleware.params$wrap_params$fn__6291.invoke(params.clj:55)\n\tat ring.middleware.multipart_params$wrap_multipart_params$fn__6386.invoke(multipart_params.clj:103)\n\tat ring.middleware.flash$wrap_flash$fn__6675.invoke(flash.clj:14)\n\tat ring.middleware.session$wrap_session$fn__6664.invoke(session.clj:43)\n\tat ring.middleware.cookies$wrap_cookies$fn__6595.invoke(cookies.clj:160)\n\tat ring.adapter.jetty$proxy_handler$fn__6112.invoke(jetty.clj:16)\n\tat ring.adapter.jetty.proxy$org.mortbay.jetty.handler.AbstractHandler$0.handle(Unknown Source)\n\tat org.mortbay.jetty.handler.HandlerWrapper.handle(HandlerWrapper.java:152)\n\tat org.mortbay.jetty.Server.handle(Server.java:326)\n\tat org.mortbay.jetty.HttpConnection.handleRequest(HttpConnection.java:542)\n\tat org.mortbay.jetty.HttpConnection$RequestHandler.headerComplete(HttpConnection.java:928)\n\tat org.mortbay.jetty.HttpParser.parseNext(HttpParser.java:549)\n\tat org.mortbay.jetty.HttpParser.parseAvailable(HttpParser.java:212)\n\tat org.mortbay.jetty.HttpConnection.handle(HttpConnection.java:404)\n\tat org.mortbay.jetty.bio.SocketConnector$Connection.run(SocketConnector.java:228)\n\tat org.mortbay.thread.QueuedThreadPool$PoolThread.run(QueuedThreadPool.java:582)\n"
}
```
