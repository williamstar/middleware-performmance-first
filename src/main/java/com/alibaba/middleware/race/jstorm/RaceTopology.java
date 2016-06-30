package com.alibaba.middleware.race.jstorm;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.jstorm.utils.JStormUtils;
import com.alibaba.middleware.race.RaceConfig;
import com.alibaba.middleware.race.TopologyUtils;
import com.alibaba.middleware.race.jstorm.bolt.PaySortBolt;
import com.alibaba.middleware.race.jstorm.bolt.TaoSortBolt;
import com.alibaba.middleware.race.jstorm.bolt.TmallSortBolt;
import com.alibaba.middleware.race.jstorm.spout.MQSpout;

import backtype.storm.Config;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;



public class RaceTopology {

	private static Logger LOG = LoggerFactory.getLogger(RaceTopology.class);

	public static void main(String[] args) throws Exception {
	
		Config conf = new Config();
		TopologyBuilder builder = new TopologyBuilder();
		builder.setSpout("spout", new MQSpout(),1);
		builder.setBolt("taoSortBolt", new TaoSortBolt(),4).localOrShuffleGrouping("spout", TopologyUtils.TAOBAO_TOPIC_STREAM);
		builder.setBolt("tmallSortBolt", new TmallSortBolt(),4).localOrShuffleGrouping("spout", TopologyUtils.TMALL_TOPIC_STREAM);
		builder.setBolt("paySortBolt", new PaySortBolt(),8).localOrShuffleGrouping("spout", TopologyUtils.PAY_TOPIC_STREAM);
	
//		builder.setBolt("taoCountBolt", new TaoCountBolt(),3).fieldsGrouping ("paySortBolt", TopologyUtils.TAOBAO_SORT_STREAM,new Fields("index"));
//		builder.setBolt("tmallCountBolt", new TmallCountBolt(),3).fieldsGrouping("paySortBolt", TopologyUtils.TMALL_SORT_STREAM,new Fields("index"));
//		builder.setBolt("PCCountBolt", new PCCountBolt(),3).fieldsGrouping("paySortBolt", TopologyUtils.PC_SORT_STREAM,new Fields("index"));
//		builder.setBolt("MobileCountBolt", new MobileCountBolt(),3).fieldsGrouping("paySortBolt", TopologyUtils.MOBILE_SORT_STREAM,new Fields("index"));
		
		String topologyName = RaceConfig.JstormTopologyName;
		conf.setDebug(false);
        int ackerNum = JStormUtils.parseInt(
                conf.get(Config.TOPOLOGY_ACKER_EXECUTORS), 1);
        Config.setNumAckers(conf, ackerNum);
		//本地调试模式
//		LocalCluster cluster = new LocalCluster();
//		conf.put(Config.TOPOLOGY_MAX_TASK_PARALLELISM, 1);
//		cluster.submitTopology(topologyName, conf, builder.createTopology());
		//集群模式
		conf.put(Config.STORM_CLUSTER_MODE, "distributed");
		conf.setNumWorkers(3);
        StormSubmitter.submitTopology(topologyName, conf,builder.createTopology());
        LOG.info("#################拓扑提交成功！###################");
	}
}
