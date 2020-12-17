package stormflow;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

public class LogsClean
        implements IRichBolt
{
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    public static boolean isNumeric(String str)
    {
        Pattern pattern = Pattern.compile("[0-9]*");
        return pattern.matcher(str).matches();
    }

    public void execute(Tuple input)
    {
        String sentence = input.getString(0);
        String[] logs = sentence.split(" ");
        if ((logs.length >= 7)
                && (!"".equals(logs[0]))
                && (logs[0] != null)
                && (!"".equals(logs[3].substring(1)))
                && (logs[3].substring(1) != null)
                && (!"".equals(logs[6])) && (logs[6] != null))
        {
            String ip = logs[0];
            String time = logs[3].substring(1);
            String request = logs[6];
            if ((ip.split("\\.", -1).length == 4) && (time.split("/", -1).length == 3)) {
                try
                {
                    Date date = this.df.parse(time);
                    Calendar cal = Calendar.getInstance();
                    cal.setTime(date);
                    long timestamp = cal.getTimeInMillis() / 1000L;
                    if ((isNumeric(ip.split("\\.", -1)[0])) && (isNumeric(ip.split("\\.", -1)[1])) && (isNumeric(ip.split("\\.", -1)[2])) && (isNumeric(ip.split("\\.", -1)[3])))
                    {
                        String ip_num = String.valueOf(Long.parseLong(ip.split("\\.", -1)[0]) * 16777216L + Long.parseLong(ip.split("\\.", -1)[1]) * 65536L + Long.parseLong(ip.split("\\.", -1)[2]) * 256L + Long.parseLong(ip.split("\\.", -1)[3]));
                        JSONObject value = new JSONObject();
                        value.put("ip", ip);
                        value.put("server_timestamp", String.valueOf(timestamp));
                        value.put("request", request);
                        this.collector.emit(input, new Values(new Object[] { ip_num, String.valueOf(value) }));
                    }
                }
                catch (ParseException e)
                {
                    e.printStackTrace();
                }
            }
        }
        this.collector.ack(input);
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(new String[] { "key", "result" }));
    }

    public Map<String, Object> getComponentConfiguration()
    {
        return null;
    }
}
