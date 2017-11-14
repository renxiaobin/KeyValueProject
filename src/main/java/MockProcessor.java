import cn.helium.kvstore.processor.Processor;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MockProcessor implements Processor {

    Map<String,Map<String,String>> store = new HashMap<String, Map<String, String>>();

    public Map<String, String> get(String s) {
        Map<String,String> record = store.get(s);
        return record;
    }

    public synchronized boolean put(String s, Map<String, String> map) {
        store.put(s,map);
        return true;
    }

    public synchronized boolean batchPut(Map<String, Map<String, String>> map) {
        store.putAll(map);
        return true;
    }

    public int count(Map<String, String> map) {
        return map.size();
    }

    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        Map<Map<String, String>, Integer> result = new HashMap<Map<String, String>, Integer>();
        return result;
    }

    public byte[] process(byte[] bytes) {
        System.out.println("receiver bytes = [" + new String(bytes) + "]");
        return "received!".getBytes();
    }
}
