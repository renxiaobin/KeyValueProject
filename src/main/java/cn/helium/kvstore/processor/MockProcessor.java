package cn.helium.kvstore.processor;


import cn.helium.kvstore.common.KvStoreConfig;
import cn.helium.kvstore.rpc.RpcServer;
import cn.helium.kvstore.util.HDFSUtils;
import cn.helium.kvstore.rpc.RpcClientFactory;


import org.apache.hadoop.conf.Configuration;
import org.jboss.netty.util.internal.ConcurrentHashMap;
import org.mortbay.util.ajax.JSON;

import static cn.helium.kvstore.util.StrUtils.isEmpty;

import java.io.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Copyright @ 2017 unknown.yuzhouwan.com
 * All right reserved.
 * Function: KV Store Processor
 *
 * @author Grace Koo
 * @since 2017/10/12 0012
 */
public class MockProcessor implements Processor {

    // private Logger _log = LoggerFactory.getLogger(KVStoreProcessor.class);

    private static final String LOCAL_DISK = "/Users/renxiaobin/opt/localdisk";
    //private static final String LOG_FILE_FILENAME = "log.txt";
    //private static final String LOG_FILE_PATH = LOCAL_DISK.concat("/").concat(LOG_FILE_FILENAME);
    //private volatile static File LOG_FILE = new File(LOG_FILE_PATH);

    private static final String HDFS_STORE_FILE = "/kvstore";

    private static final String HDFS_URL = "hdfs://localhost:8020";
    private ConcurrentHashMap<String, Map<String, String>> store = new ConcurrentHashMap<>();
    private static final String STORE_FILENAME = "store.txt";
    private static final String STORE_FILE_PATH = LOCAL_DISK.concat("/").concat(STORE_FILENAME);
    private static final File STORE_FILE = new File(STORE_FILE_PATH);

    public MockProcessor() {
    }

    @Override
    public Map<String, String> get(String key) {
        String log = String.format("KVStoreProcessor get method, key: %s", key);
        //saveLog2LocalDisk(log);
        if (isEmpty(key)) {
            // _log.warn("Key cannot be empty!");
            log = "Key cannot be empty!";
            System.out.println(log);
            //saveLog2LocalDisk(log);
            return null;
        }
        if (!store.containsKey(key)) {
            // _log.warn("Cannot find key form map, key = {}!", key);
            log = String.format("Cannot find key form map, key = %s!", key);
            System.out.println(log);
            //saveLog2LocalDisk(log);

            loadFromHDFS();
            if (!store.containsKey(key)) {
                log = "Cannot get key from hdfs!";
                System.out.println(log);
                //saveLog2LocalDisk(log);
                return null;
            }
        }
        return store.get(key);
    }

    @Override
    public synchronized boolean put(String key, Map<String, String> value) {
        String log = String.format("KVStoreProcessor put method, key: %s, value: %s", key, JSON.toString(value));
        //saveLog2LocalDisk(log);
        System.out.println(log);
        store.put(key, value);
        return !internalPut();
    }

    @Override
    public synchronized boolean batchPut(Map<String, Map<String, String>> records) {
        String log = String.format("KVStoreProcessor batchPut method, records: %s", JSON.toString(records));
        //saveLog2LocalDisk(log);
        System.out.println(log);
        store.putAll(records);
        return !internalPut();
    }

    private boolean internalPut() {
        boolean saved = saveStore2LocalDisk(store);
        if (!saved)
            return true;

        HDFSUtils client = new HDFSUtils();
        Configuration conf = new Configuration();
        String hdfsPath = HDFS_URL + HDFS_STORE_FILE/* + RpcServer.getRpcServerId() */;
        conf.set("fs.default.name", hdfsPath);
        try {
            client.deleteFile(hdfsPath, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            client.addFile(STORE_FILE_PATH, hdfsPath, conf);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return false;
    }

    @Override
    public int count(Map<String, String> map) {
        loadFromHDFS();
        String log = String.format("KVStoreProcessor count method, records: %s", JSON.toString(map));
        //saveLog2LocalDisk(log);
        System.out.println(log);
        int count = 0;
        String key, value;
        Map<String, String> internalMap;
        for (Map.Entry<String, String> entry : map.entrySet()) {
            key = entry.getKey();
            value = entry.getValue();
            if (store.containsKey(key)) {
                internalMap = store.get(key);
                if (internalMap.containsKey(value))
                    count++;
            }
        }
        return count;
    }

    @Override
    public Map<Map<String, String>, Integer> groupBy(List<String> list) {
        loadFromHDFS();
        String log = String.format("KVStoreProcessor groupBy method, list: %s", JSON.toString(list));
        //saveLog2LocalDisk(log);
        System.out.println(log);
        Map<Map<String, String>, Integer> counter = new HashMap<>();
        for (String l : list) {
            if (store.containsKey(l)) {
                Map<String, String> internalMap = store.get(l);
                for (Map.Entry<String, String> entry : internalMap.entrySet()) {
                    Map<String, String> map = new HashMap<>();
                    map.put(l, entry.getKey());
                    counter.put(map, 1);
                }
            }
        }
        return counter;
    }

    @Override
    public byte[] process(byte[] input) {
        String log = String.format("KVStoreProcessor process method, input: %s", new String(input));
        //saveLog2LocalDisk(log);
        System.out.println(log);
        int num = KvStoreConfig.getServersNum();
        int index = RpcServer.getRpcServerId();

        for (int i = 1; i < num; i++) {
            if (i != index)
                try {
                    RpcClientFactory.inform(i, input);
                } catch (IOException e) {
                    e.printStackTrace();
                }
        }

        return input;
    }

	/*private void saveLog2LocalDisk(String log) {
        System.out.println(log);
		try (BufferedOutputStream bufferedInputStream = new BufferedOutputStream(new FileOutputStream(LOG_FILE))) {
			bufferedInputStream.write(log.getBytes());
			bufferedInputStream.write("\n".getBytes());
		} catch (IOException e) {
			e.printStackTrace();
		}
	}*/

    private boolean saveStore2LocalDisk(ConcurrentHashMap<String, Map<String, String>> store) {
        STORE_FILE.delete();
        try {
            boolean created = STORE_FILE.createNewFile();
            if (!created) {
                System.out.println("Cannot create store file on local disk!");
                return false;
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        try (BufferedOutputStream bufferedInputStream = new BufferedOutputStream(new FileOutputStream(STORE_FILE))) {
            String storeJSON = JSON.toString(store);
            bufferedInputStream.write(storeJSON.getBytes());
            bufferedInputStream.write("\n".getBytes());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }

    private void loadFromHDFS() {
        HDFSUtils client = new HDFSUtils();
        Configuration conf = new Configuration();
        String hdfsPath = HDFS_URL + HDFS_STORE_FILE + RpcServer.getRpcServerId();
        conf.set("fs.default.name", hdfsPath);
        try {
            client.readFile(STORE_FILE_PATH, hdfsPath, conf);
            loadStoreFromLocalDisk();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    private void loadStoreFromLocalDisk() {
        byte[] buffer = new byte[1024];
        int len = 0;
        try (BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(STORE_FILE))) {
            StringBuilder builder = new StringBuilder();
            while ((len = bufferedInputStream.read(buffer)) > 0) {
                builder.append(new String(buffer, 0, len));
            }

            ConcurrentHashMap<String, Map<String, String>> concurrentHashMap = new ConcurrentHashMap((HashMap)JSON.parse(builder.toString()));
            store = concurrentHashMap;
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}

