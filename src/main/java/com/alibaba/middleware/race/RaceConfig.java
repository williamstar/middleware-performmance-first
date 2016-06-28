package com.alibaba.middleware.race;

public class RaceConfig {

    //这些是写tair key的前缀
    public static String prex_tmall = "platformTmall_TeamCode_";
    public static String prex_taobao = "platformTaobao_TeamCode_";
    public static String prex_ratio = "ratio_TeamCode_";

    
    //这些jstorm/rocketMq/tair 的集群配置信息，这些配置信息在正式提交代码前会被公布
    public static String JstormTopologyName = "xxx";
    public static String MetaConsumerGroup = "xxx";
    public static String MqPayTopic = "MiddlewareRaceTestData_Pay";
    public static String MqTmallTradeTopic = "MiddlewareRaceTestData_TMOrder";
    public static String MqTaobaoTradeTopic = "MiddlewareRaceTestData_TBOrder";
    public static String TairConfigServer = "192.168.1.149:5198";
    public static String TairSalveConfigServer = "xxx";
    public static String TairGroup = "group_1";
    public static Integer TairNamespace = 0;
    
    //在本地搭建好broker后,记得指定nameServer的地址
   // public static String NamesrvAddr = "192.168.1.149:9876";
}
