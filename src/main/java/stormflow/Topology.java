package stormflow;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.AlreadyAliveException;
import backtype.storm.generated.InvalidTopologyException;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import storm.kafka.*;

import java.util.ArrayList;

public class Topology
{
    public static void main(String[] args)
            throws AlreadyAliveException, InvalidTopologyException, InterruptedException
    {
        String zks = "node11:2181,hadoop25:2181,node13:2181,node14:2181,node15:2181,node16:2181,node17:2181";
        String topic = "realflow3";
        String zkRoot = "/zkspout13";
        String id = "zklogs3";
        BrokerHosts brokerHosts = new ZkHosts(zks);
        SpoutConfig spoutConf = new SpoutConfig(brokerHosts, topic, zkRoot, id);

        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());
        spoutConf.forceFromStart = false;
        spoutConf.fetchSizeBytes = 1048576;
        spoutConf.socketTimeoutMs = 10000;

        spoutConf.bufferSizeBytes = 1048576;

        spoutConf.zkServers = new ArrayList() {};
        spoutConf.zkPort = Integer.valueOf(2181);

        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout("reader", new KafkaSpout(spoutConf));
        builder.setBolt("logs-clean", new LogsClean()).shuffleGrouping("reader");
        builder.setBolt("logs-concat", new LogsConcat(), Integer.valueOf(10)).fieldsGrouping("logs-clean", new Fields(new String[] { "key" }));
        builder.setBolt("logs-concat-store", new LogsConcatStore(), Integer.valueOf(10)).shuffleGrouping("logs-concat");

        Config conf = new Config();

        conf.setMessageTimeoutSecs(80);
        if ((args != null) && (args.length > 0))
        {
            String nimbus = args[0];
            conf.put("nimbus.host", nimbus);
            conf.setNumWorkers(1);
            StormSubmitter.submitTopology("Topology1", conf, builder.createTopology());
        }
        else
        {
            conf.setMaxTaskParallelism(3);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("local-host", conf, builder.createTopology());
            Thread.sleep(10000L);
            cluster.shutdown();
        }
    }
}
