package com.tencent.jstorm.tube;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;

/**
 * @author <a href="mailto:caofangkun@gmail.com">caokun</a>
 * @author <a href="mailto:xunzhang555@gmail.com">zhangxun</a>
 * 
 */
public class TubeSpout extends BaseRichSpout {

	private static final long serialVersionUID = 1L;
	public static final Logger LOG = LoggerFactory.getLogger(TubeSpout.class);

	SpoutConfig _spoutConf;
	SpoutOutputCollector _collector;

	public TubeSpout(SpoutConfig spoutConf) {
		_spoutConf = spoutConf;
	}

	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		_collector = collector;
		Map stateConf = new HashMap(conf);

	}

	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub

	}

}
