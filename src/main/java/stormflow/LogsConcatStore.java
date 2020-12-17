package stormflow;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Tuple;
import com.alibaba.fastjson.JSONObject;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class LogsConcatStore
        implements IRichBolt
{
    Log LOG = LogFactory.getLog(LogsConcatStore.class);
    private static final long serialVersionUID = 1L;
    Integer id;
    String name;
    Map<String, Integer> counters;
    SimpleDateFormat df1 = new SimpleDateFormat("yyyyMMdd");
    SimpleDateFormat df2 = new SimpleDateFormat("HH");
    SimpleDateFormat df3 = new SimpleDateFormat("mm");
    String lastdate = "";
    private OutputCollector collector;

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.counters = new HashMap();
        this.collector = collector;
        this.name = context.getThisComponentId();
        this.id = Integer.valueOf(context.getThisTaskId());
    }

    public void execute(Tuple input)
    {
        String request = input.getString(0);
        JSONObject jsonObject = JSONObject.parseObject(request);
        String ip_s = jsonObject.getString("ip") == null ? "" : jsonObject.getString("ip");
        String server_time_s = jsonObject.getString("server_timestamp") == null ? "" : jsonObject.getString("server_timestamp");
        String request_s = jsonObject.getString("request") == null ? "" : jsonObject.getString("request");
        //根据server_timestamp将日志写入对应文件
        if ((!"".equals(server_time_s)) && (!"".equals(ip_s)) && (!"".equals(request_s)))
        {
            String date = this.df1.format(new Date(Long.parseLong(server_time_s) * 1000L));
            String hour = this.df2.format(new Date(Long.parseLong(server_time_s) * 1000L));
            try
            {
                String path = "/data/file/storm-logs/ods_traf_weblog/" + date;
                File f = new File(path);
                if (!f.exists()) {
                    f.mkdirs();
                }
                String path1 = "/data/file/storm-logs/ods_traf_weblog/" + date + "/" + date + hour + ".log";
                File file = new File(path1);
                if (!file.exists()) {
                    file.createNewFile();
                }
                FileWriter fileWritter = new FileWriter(path1, true);
                fileWritter.write(request + "\r\n");
                fileWritter.close();
            }
            catch (IOException e)
            {
                e.printStackTrace();
            }
        }
        this.collector.ack(input);
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer) {}

    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
}
