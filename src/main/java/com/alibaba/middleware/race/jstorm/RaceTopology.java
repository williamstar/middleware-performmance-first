package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.PaySortBolt;
import com.alibaba.middleware.race.jstorm.bolt.SerializableBolt;
import com.alibaba.middleware.race.jstorm.spout.MQSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;

public class RaceTopology {

	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	public static void main(String[] args) throws Exception {

		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		
		builder.setSpout("spout", new MQSpout(), 1);
		builder.setBolt("SerializableBolt", new SerializableBolt(), 18).shuffleGrouping("spout");

		builder.setBolt("paySortBolt", new PaySortBolt(),1).shuffleGrouping("SerializableBolt");

		String topologyName = RaceConfig.JstormTopologyName;
		conf.setDebug(false);
		// int ackerNum = JStormUtils.parseInt(
		// conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
		// Config.setNumAckers(conf, ackerNum);
		// 本地调试模式
//		LocalCluster cluster = new LocalCluster();
//		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
//		cluster.submitTopology(topologyName, conf, builder.createTopology());
		// 集群模式
		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		conf.setNumWorkers(3);
		StormSubmitter.submitTopology(topologyName, conf, builder.createTopology());
		LOG.info("#################拓扑提交成功！###################");
	}
}
