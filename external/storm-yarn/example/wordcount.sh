
storm-yarn jar ./storm.yaml -topologyJar ./storm-starter-0.0.1-SNAPSHOT.jar -topologyMainClass storm.starter.WordCountTopology -args wordcountTop -numWorkers 3
