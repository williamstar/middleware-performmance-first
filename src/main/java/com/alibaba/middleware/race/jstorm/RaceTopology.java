package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.jstorm.bolt.PaySortBolt;
import com.alibaba.middleware.race.jstorm.bolt.TaoSortBolt;
import com.alibaba.middleware.race.jstorm.bolt.TmallSortBolt;
import com.alibaba.middleware.race.jstorm.spout.MQPayPullSpout;
import com.alibaba.middleware.race.jstorm.spout.MQTaoPullSpout;
import com.alibaba.middleware.race.jstorm.spout.MQTmallPullSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;




public class RaceTopology {

	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);
	public static void main(String[] args) throws Exception {
	
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		
        builder.setSpout("taoSpout", new MQTaoPullSpout(),1);
        builder.setSpout("tmallSpout", new MQTmallPullSpout(),1);
		builder.setSpout("paySpout", new MQPayPullSpout(),1);
		
		builder.setBolt("taoSortBolt", new TaoSortBolt(),4).shuffleGrouping("taoSpout");
		builder.setBolt("tmallSortBolt", new TmallSortBolt(),4).shuffleGrouping("tmallSpout");
		builder.setBolt("paySortBolt", new PaySortBolt(),8).shuffleGrouping("paySpout");
	
		
		String topologyName = RaceConfig.JstormTopologyName;
		conf.setDebug(false);
//        int ackerNum = JStormUtils.parseInt(
//                conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
//        Config.setNumAckers(conf, ackerNum);
		//本地调试模式
//		LocalCluster cluster = new LocalCluster();
//		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
//		cluster.submitTopology(topologyName, conf, builder.createTopology());
		//集群模式
		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		conf.setNumWorkers(3);
		LOG.info("#################拓扑提交成功！###################");
        StormSubmitter.submitTopology(topologyName, conf,builder.createTopology());
	}
}
