# JStorm rename hack

In the latest version, the class packages have been changed from "backtype.storm" to "org.apache.storm" so the topology code compiled with older version won't run on the Storm 1.0.0 just like that. Backward compatibility is available through following configuration

  ```client.jartransformer.class: "org.apache.storm.hack.StormShadeTransformer"```

You need to add the above config in storm installation if you want to run the code compiled with older versions of storm. The config should be added in the machine you use to submit your topologies.

Refer to https://issues.apache.org/jira/browse/STORM-1202 for more details.

