set topology.name=test_topology;
set storm.jar=./jstorm-example-0.9.0.jar;
add jar jstorm-example-0.9.0.jar;
set topology.workers=3;
REGISTER spout=SPOUT("storm.starter.spout.RandomSentenceSpout", 5);
REGISTER split=BOLT("storm.starter.WordCountTopology$SplitSentence", 8).SHUFFLE("spout");
REGISTER count=BOLT("storm.starter.WordCountTopology$WordCount", 12).FIELDS("split", "word");
submit;
