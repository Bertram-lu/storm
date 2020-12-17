package stormflow;

import backtype.storm.Config;
import backtype.storm.Constants;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichBolt;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import com.alibaba.fastjson.JSONObject;

import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;

public class LogsConcat
        implements IRichBolt
{
    private static final long serialVersionUID = 1L;
    private OutputCollector collector;
    double value = 2000.0D;
    JSONObject value_json = new JSONObject();
    SimpleDateFormat df = new SimpleDateFormat("dd/MMM/yyyy:HH:mm:ss", Locale.US);
    SimpleDateFormat df1 = new SimpleDateFormat("yyyy-MM-dd");
    SimpleDateFormat df2 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    SimpleDateFormat df3 = new SimpleDateFormat("HH");

    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector)
    {
        this.collector = collector;
    }

    public static String replaceLine(String line)
    {
        line = line.replaceAll("\r", "<CR>");
        line = line.replaceAll("\n", "<LF>");
        line = line.replaceAll("\t", "<TAB>");
        line = line.replaceAll("\001", "<SOH>");
        line = line.replaceAll("\001", "<SOH>");
        line = line.replaceAll("\000", "");
        line = line.replaceAll("\005", "");
        return line;
    }

    public void execute(Tuple input)
    {   // __system Constants.SYSTEM_COMPONENT_ID; SYSTEM_TICK_STREAM_ID = "__tick";
        // 判断是否是定时任务，定时器的主要任务是清理value_json的数据
            if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID) &&
                    input.getSourceStreamId().equals(Constants.SYSTEM_TICK_STREAM_ID)) {
                long starttime=System.currentTimeMillis();
                JSONObject value=value_json;
                JSONObject jsonOut = JSONObject.parseObject(String.valueOf(value));
                for(Entry<String,Object> entry:jsonOut.entrySet()){
                    String key1=entry.getKey();
                    Map<String, String> jsonMap=(Map<String, String>) entry.getValue();
                    if(jsonMap.containsKey("server_timestamp") && jsonMap.containsKey("ip")){
                        String server_time_key=jsonMap.get("server_timestamp");
                        String ip1 = jsonMap.get("ip");
                        //服务器时间与当前时间对比大于50s的数据处理
                        if(starttime/1000-Long.parseLong(server_time_key)>=50){
                            int value2=jsonMap.size()-2; //减2 可以得到现在存储了多少段
                            //可优化成StringBuffer
                            String request2="";
                            //尽量保存完整的日志，比如1、2、3 三段日志，第二段丢了，只留了1，然后remove
                            for(int i=1;i<=value2;i++){
                                if(jsonMap.get(String.valueOf(i))!=null){
                                    request2=request2+jsonMap.get(String.valueOf(i));
                                }
                            }
                            //空值判断，发射数据 从这个json中移除该kv
                            if(!"".equals(request2)){
                                JSONObject value1 = new JSONObject();
                                value1.put("ip", ip1);
                                value1.put("server_timestamp", server_time_key);
                                value1.put("request", String.valueOf(request2));
                                collector.emit(input,new Values(replaceLine(String.valueOf(value1))));
                                value_json.remove(key1);
                            }else{
                                value_json.remove(key1);
                            }
                        }
                    }
                }
            int num = this.value_json.size();
            System.out.println("LogsConcat-json数量:" + num);
            long endtime = System.currentTimeMillis();
            System.out.println("LogsConcat-clean:" + String.valueOf(endtime - starttime));
        }
        else
        {
            String json = input.getString(1);
            JSONObject jsonObject = JSONObject.parseObject(json);
            String ip = jsonObject.getString("ip") == null ? "" : jsonObject.getString("ip");
            String server_timestamp = jsonObject.getString("server_timestamp") == null ? "" : jsonObject.getString("server_timestamp");
            String request = jsonObject.getString("request") == null ? "" : jsonObject.getString("request");
            if ((!"".equals(ip)) && (!"".equals(server_timestamp)) && (!"".equals(request))) {
                // 如果是非切割文件直接发射
                if ((request.startsWith("/b.gif?")) || (request.startsWith("/k.gif?")) || (request.startsWith("/t.gif?")) || (request.startsWith("/a.gif?")) || (request.startsWith("/c.gif?"))||(request.startsWith("/g.gif?")))
                {
                    JSONObject value = new JSONObject();
                    value.put("ip", ip);
                    value.put("server_timestamp", server_timestamp);
                    value.put("request", String.valueOf(request));
                    this.collector.emit(input, new Values(new Object[] { replaceLine(String.valueOf(value)) }));
                }
                // l.gif 说明是切割的文件
                else if ((request.startsWith("/l.gif?n=b")) || (request.startsWith("/l.gif?n=k")) || (request.startsWith("/l.gif?n=t")) || (request.startsWith("/l.gif?n=a")) || (request.startsWith("/l.gif?n=c")))
                {
                    long starttime3 = System.currentTimeMillis();
                    String request_v1 = "";
                    String request_v2 = "";
                    String request_n = "";
                    String request_n1 = "";
                    String request_n2 = "";
                    String request_n3 = "";
                    String request_n4 = "";
                    String request_k = "";
                    String request_k1 = "";
                    String request_k2 = "";
                    String request_k3 = "";
                    if (request.split("&v=").length == 2)
                    {
                        request_v1 = request.split("&v=")[0];  // /l.gif?n=t.2275&k=ipln8s9meke6pbka.1
                        request_v2 = request.split("&v=")[1];  //t_track=show_m_toutiao_article_tonglan_zb&id=2-162fc22ywt6zbfrf-1598593494791---1598593494791-1598593494791--1598593494791-1&t1=1598593494787&t=1598593495248&lh=http%3A%2F%2Fm.jia.com%2Fzixun%2Fjxwd%2F632408.html&r0=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1001192y%2Fssid%3D0%2Fuid%3D0%2Fbd_page_type%3D1%2Fpu%3Dusm%25404%252Csz%2540320_1001%252Cta%2540iphone_2_10.0_24_78.0%2Fbaiduid%3DF68DC27F6930D900C69A460F26C80722%2Fw%3D0_10_%2Ft%3Diphone%2Fl%3D1%2Ftc%3Fclk_type%3D1%26vit%3Dosres%26l%3D1%26baiduid%3DF68DC27F6930D900C69A460F26C80722%26t%3Diphone%26ref%3Dwww_iphone%26from%3D1001192y%26ssid%3D0%26lid%3D12186235268859391803%26bd_page_type%3D1%26pu%3Dusm%25404%252Csz%2540320_1001%252Cta%2540iphone_2_10.0_24_78.0%26order%3D1%26fm%3Dalop%26cyc%3D1%26isAtom%3D1%26is_baidu%3D0%26tj%3D9za_1_0_10_lNaN%26clk_info%3D%257B%2522tplname%2522%253A%2522wenda_abstract%2522%252C%2522srcid%2522%253A36402%252C%2522t%2522%253A1598593494106%252C%2522xpath%2522%253A%2522div-article-section-div-div5-div-div-div(lgtt)-div-div-span%2522%257D%26wd%3D%26eqid%3Da91e34695db36f3b100000005f4899ca%26w_qd%3DIlPT2AEptyoA_yky-Roa6equJ6pUi7kmzi5NgPUDQnwtNuovPFVa8mu%26bdver%3D2%26tcplug%3D1%26dict%3D-1%26sec%3D6053%26di%3D2dedf16eaeae2703%26bdenc%3D1%26nsrc%3DmGIc5BoT1UzcoVjz6oeMR1Ji1vl9m6KELql0GtcFfc%252FkNiA2eOuzRbvx1GRUt%252B4V0N09ByNeHhgsrA48rHRbaEBLmmwz%252F8DxqcCfS%252BEgzFA%253D&pi=c1azypywdsurkcsk&ri=2wi7j2587uo1emtd&bs=imheggvvnx841yn2&bl=zh-CN&sw=360&sh=780&sc=24&pf=Linux%20armv8l&ic=t&ij=f&jv=1.8.5&wt=t&p=0-0-1598593494296-0-----59-64-77-77-88--88-103-106-117-726-726-821---&dc=UTF-8&dt=%E7%A9%BA%E7%BD%AE%E6%88%BF%E7%89%A9%E4%B8%9A%E8%B4%B970%25%E7%9A%84%E4%BE%9D%E6%8D%AE_%E9%BD%90%E5%AE%B6%E7%BD%91&ua=Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20ELE-AL00%3B%20HMSCore%205.0.1.313%3B%20GMSCore%2020.15.16)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F78.0.3904.108%20HuaweiBrowser%2F10.1.4.301%20Mobile%20Safari%2F537.36&r1=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1001192y%2Fssid%3D0%2Fuid%
                    }
                    if ((!request_v1.equals("")) &&
                            (request_v1.split("&").length == 2))
                    {
                        request_n = request_v1.split("&")[0];  // /l.gif?n=t.2275
                        request_k = request_v1.split("&")[1];  // k=ipln8s9meke6pbka.1
                    }
                    if ((!request_n.equals("")) &&
                            (request_n.split("n=").length == 2))
                    {
                        request_n1 = request_n.split("n=")[0];// /l.gif?
                        request_n2 = request_n.split("n=")[1];// t.2275
                    }
                    if ((!request_n2.equals("")) &&
                            (request_n2.split("\\.").length == 2))
                    {
                        request_n3 = request_n2.split("\\.")[0]; // t
                        request_n4 = request_n2.split("\\.")[1]; // 2275
                    }
                    if ((!request_k.equals("")) &&
                            (request_k.split("k=").length == 2)) { //split后list[0]="" list[1]="ipln8s9meke6pbka.1"
                        request_k1 = request_k.split("k=")[1];  //ipln8s9meke6pbka.1
                    }
                    if ((!request_k1.equals("")) &&
                            (request_k1.split("\\.").length == 2))
                    {
                        request_k2 = request_k1.split("\\.")[0]; //ipln8s9meke6pbka
                        request_k3 = request_k1.split("\\.")[1]; // 1
                    }
                    if ((!"".equals(request_k2)) && (!"".equals(request_k3)) && (!"".equals(request_n1)) && (!"".equals(request_n3)) && (!"".equals(request_n4)) && (!"".equals(request_v2)))
                    {
                        String value_r = "";
                        //是第一段日志value_r=t.gif?t_track=show_m_toutiao_article_tonglan_zb&id=2-162fc22ywt6zbfrf-1598593494791---1598593494791-1598593494791--1598593494791-1&t1=1598593494787&t=1598593495248&lh=http%3A%2F%2Fm.jia.com%2Fzixun%2Fjxwd%2F632408.html&r0=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1001192y%2Fssid%3D0%2Fuid%3D0%2Fbd_page_type%3D1%2Fpu%3Dusm%25404%252Csz%2540320_1001%252Cta%2540iphone_2_10.0_24_78.0%2Fbaiduid%3DF68DC27F6930D900C69A460F26C80722%2Fw%3D0_10_%2Ft%3Diphone%2Fl%3D1%2Ftc%3Fclk_type%3D1%26vit%3Dosres%26l%3D1%26baiduid%3DF68DC27F6930D900C69A460F26C80722%26t%3Diphone%26ref%3Dwww_iphone%26from%3D1001192y%26ssid%3D0%26lid%3D12186235268859391803%26bd_page_type%3D1%26pu%3Dusm%25404%252Csz%2540320_1001%252Cta%2540iphone_2_10.0_24_78.0%26order%3D1%26fm%3Dalop%26cyc%3D1%26isAtom%3D1%26is_baidu%3D0%26tj%3D9za_1_0_10_lNaN%26clk_info%3D%257B%2522tplname%2522%253A%2522wenda_abstract%2522%252C%2522srcid%2522%253A36402%252C%2522t%2522%253A1598593494106%252C%2522xpath%2522%253A%2522div-article-section-div-div5-div-div-div(lgtt)-div-div-span%2522%257D%26wd%3D%26eqid%3Da91e34695db36f3b100000005f4899ca%26w_qd%3DIlPT2AEptyoA_yky-Roa6equJ6pUi7kmzi5NgPUDQnwtNuovPFVa8mu%26bdver%3D2%26tcplug%3D1%26dict%3D-1%26sec%3D6053%26di%3D2dedf16eaeae2703%26bdenc%3D1%26nsrc%3DmGIc5BoT1UzcoVjz6oeMR1Ji1vl9m6KELql0GtcFfc%252FkNiA2eOuzRbvx1GRUt%252B4V0N09ByNeHhgsrA48rHRbaEBLmmwz%252F8DxqcCfS%252BEgzFA%253D&pi=c1azypywdsurkcsk&ri=2wi7j2587uo1emtd&bs=imheggvvnx841yn2&bl=zh-CN&sw=360&sh=780&sc=24&pf=Linux%20armv8l&ic=t&ij=f&jv=1.8.5&wt=t&p=0-0-1598593494296-0-----59-64-77-77-88--88-103-106-117-726-726-821---&dc=UTF-8&dt=%E7%A9%BA%E7%BD%AE%E6%88%BF%E7%89%A9%E4%B8%9A%E8%B4%B970%25%E7%9A%84%E4%BE%9D%E6%8D%AE_%E9%BD%90%E5%AE%B6%E7%BD%91&ua=Mozilla%2F5.0%20(Linux%3B%20Android%2010%3B%20ELE-AL00%3B%20HMSCore%205.0.1.313%3B%20GMSCore%2020.15.16)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F78.0.3904.108%20HuaweiBrowser%2F10.1.4.301%20Mobile%20Safari%2F537.36&r1=https%3A%2F%2Fm.baidu.com%2Ffrom%3D1001192y%2Fssid%3D0%2Fuid%
                        //不是第一段日志不拼接t.gif等
                        if (request_k3.equals("1")) {
                            value_r = request_n1.replace("l", request_n3) + request_v2;
                        } else {
                            value_r = request_v2;
                        }
                        String key_r = request_k2; //ipln8s9meke6pbka
                        int value1 = 0;
                        try
                        {
                            value1 = (int)Math.ceil(Integer.parseInt(request_n4) / this.value); // value1=日志长度/2000向上取整，即value1=切割数
                        }
                        catch (Exception localException) {}
                        JSONObject jsonIn = JSONObject.parseObject(String.valueOf(this.value_json));
                        Map<String, String> jsonMapIn = (Map)jsonIn.get(key_r);
                        //根据key值获取value_json中的json，切分的日志key值相同，这步操作实际上是拼接切割的字符串
                        if (jsonMapIn == null)
                        {
                            Map<String, String> map_value = new HashMap();
                            map_value.put(request_k3, value_r); //"1":"show_m_toutiao_article_tonglan_zb..."
                            map_value.put("ip", ip);
                            map_value.put("server_timestamp", String.valueOf(server_timestamp));
                            this.value_json.put(key_r, map_value);
                        }
                        else
                        {   //取切割日志中最小的server_timestamp 作为 server_timestamp
                            String server_time_map = (String)jsonMapIn.get("server_timestamp");
                            if ((server_time_map != null) && (Long.parseLong(server_time_map) > Long.parseLong(server_timestamp))) {
                                jsonMapIn.put("server_timestamp", String.valueOf(server_timestamp));
                            }
                            //jsonMapIn{"ip":ip,"server_timestamp":server_timestamp,"1":v1,"2":v2,...}
                            jsonMapIn.put(request_k3, value_r); // 段号=body
                            int value2 = jsonMapIn.size() - 2;  // 通过map大小判断是否最后一段 避免段丢失造成误判
                            //判断当前处理的这一段是否是最后一段，传完了就发射，否则put进value_json
                            if (value1 == value2)
                            {
                                String request2 = "";
                                for (int i = 1; i <= value2; i++) {
                                    if (jsonMapIn.get(String.valueOf(i)) != null) {
                                        request2 = request2 + (String)jsonMapIn.get(String.valueOf(i)); //将每段的body进行拼接
                                    }
                                }
                                if (!"".equals(request2))
                                {
                                    String ip2 = (String)jsonMapIn.get("ip");
                                    String server_time2 = (String)jsonMapIn.get("server_timestamp");
                                    JSONObject value = new JSONObject();
                                    value.put("request", request2);
                                    value.put("ip", ip2);
                                    value.put("server_timestamp", server_time2);
                                    this.collector.emit(input, new Values(new Object[] { replaceLine(String.valueOf(value)) }));
                                    this.value_json.remove(key_r);
                                }
                                else
                                {
                                    this.value_json.put(key_r, jsonMapIn);
                                }
                            }
                            //非最后一段和第一段处理
                            else
                            {
                                this.value_json.put(key_r, jsonMapIn);
                            }
                        }
                    }
                    long endtime3 = System.currentTimeMillis();
                    System.out.println("LogsConcat-lgif:" + String.valueOf(endtime3 - starttime3));
                }
            }
        }
        this.collector.ack(input);
    }

    public void cleanup() {}

    public void declareOutputFields(OutputFieldsDeclarer declarer)
    {
        declarer.declare(new Fields(new String[] { "result" }));
    }

    public Map<String, Object> getComponentConfiguration()
    {
        Config conf = new Config();
        conf.put("topology.tick.tuple.freq.secs", Integer.valueOf(60));
        return conf;
    }
}
