package de.caluga.morphium.influxdb;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bson.MorphiumId;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.apache.http.*;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URLEncodedUtils;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.SocketConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

/**
 * Created by stephan on 22.06.16.
 */
public class InfluxDbDriver implements MorphiumDriver {
    private int maxConPerHost=1;
    private int minConPerHost=1;
    private int socketTimeout=0;
    private int conTimeout=5000;
    private int heartbeatFrequency=1000;
    private Logger log=new Logger(InfluxDbDriver.class);
    private PoolingHttpClientConnectionManager conMgr;

    private String[] hosts;

    private ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
        public long getKeepAliveDuration(HttpResponse response, HttpContext context) {
            HeaderElementIterator it = new BasicHeaderElementIterator
                    (response.headerIterator(HTTP.CONN_KEEP_ALIVE));
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase
                        ("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 5 * 1000;
        }
    };

    public void setCredentials(String db, String login, char[] pwd) {

    }

    public boolean isReplicaset() {
        return false;
    }

    public String[] getCredentials(String db) {
        return new String[0];
    }

    public boolean isDefaultFsync() {
        return false;
    }

    public String[] getHostSeed() {
        return hosts;
    }

    public int getMaxConnectionsPerHost() {
        return maxConPerHost;
    }

    public int getMinConnectionsPerHost() {
        return 0;
    }

    public int getMaxConnectionLifetime() {
        return 0;
    }

    public int getMaxConnectionIdleTime() {
        return 0;
    }

    public int getSocketTimeout() {
        return 0;
    }

    public int getConnectionTimeout() {
        return 0;
    }

    public int getDefaultW() {
        return 0;
    }

    public int getMaxBlockintThreadMultiplier() {
        return 0;
    }

    public int getHeartbeatFrequency() {
        return 0;
    }

    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    public void setUseSSL(boolean useSSL) {

    }

    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    public void setWriteTimeout(int writeTimeout) {

    }

    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    public void setCredentials(Map<String, String[]> credentials) {

    }

    public int getHeartbeatSocketTimeout() {
        return 0;
    }

    public boolean isUseSSL() {
        return false;
    }

    public boolean isDefaultJ() {
        return false;
    }

    public int getWriteTimeout() {
        return 0;
    }

    public int getLocalThreshold() {
        return 0;
    }

    public void setHostSeed(String... host) {
        hosts=host;
    }

    public void setMaxConnectionsPerHost(int mx) {

    }

    public void setMinConnectionsPerHost(int mx) {

    }

    public void setMaxConnectionLifetime(int timeout) {

    }

    public void setMaxConnectionIdleTime(int time) {

    }

    public void setSocketTimeout(int timeout) {

    }

    public void setConnectionTimeout(int timeout) {

    }

    public void setDefaultW(int w) {

    }

    public void setMaxBlockingThreadMultiplier(int m) {

    }

    public void heartBeatFrequency(int t) {

    }

    public void heartBeatSocketTimeout(int t) {

    }

    public void useSsl(boolean ssl) {

    }

    public void connect() throws MorphiumDriverException {
        conMgr=new PoolingHttpClientConnectionManager(getMaxConnectionLifetime(),TimeUnit.MILLISECONDS);
        conMgr.setDefaultMaxPerRoute(100);
        conMgr.setMaxTotal(100000);
    }

    public void setDefaultReadPreference(ReadPreference rp) {

    }

    public void connect(String replicasetName) throws MorphiumDriverException {
        connect();
    }

    public Maximums getMaximums() {
        return null;
    }

    public boolean isConnected() {
        return false;
    }

    public void setDefaultJ(boolean j) {

    }

    public void setDefaultWriteTimeout(int wt) {

    }

    public int getDefaultWriteTimeout() {
        return 0;
    }

    public void setLocalThreshold(int thr) {

    }

    public void setDefaultFsync(boolean j) {

    }

    public void setRetriesOnNetworkError(int r) {

    }

    public int getRetriesOnNetworkError() {
        return 0;
    }

    public void setSleepBetweenErrorRetries(int s) {

    }

    public int getSleepBetweenErrorRetries() {
        return 0;
    }

    public void close() throws MorphiumDriverException {

    }

    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> getDBStats(String db) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> getOps(long threshold) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> runCommand(String db, Map<String, Object> cmd) throws MorphiumDriverException {
        return null;
    }

    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return null;
    }

    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        return null;
    }

    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {

    }

    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        return null;
    }

    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        return 0;
    }

    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        store(db,collection,objs,wc);
    }

    public void store(String db, String collection, List<Map<String, Object>> list, WriteConcern writeConcern) throws MorphiumDriverException {

        StringBuilder b = new StringBuilder();
        b.append(collection).append(",");

        //assuming number and boolean values as influx values
        //all others as tags

        for (Map<String, Object> measurement : list) {
            long tm = System.nanoTime();


            if (measurement.get("_id") != null) {
//                log.warn("Cannot up´date values in influxdb! Will create a new entry!");
                try {
                    tm = Long.valueOf(measurement.get("_id").toString());
                } catch (NumberFormatException e) {
                    log.warn("could not read timestamp from _id field! Assuming now!");
                }
            }


            List<String> valueKeys = new ArrayList<String>();
            for (Map.Entry<String, Object> entry : measurement.entrySet()) {

                if (entry.getKey().equals("_id")) continue;
                if (entry.getValue() instanceof Number || entry.getValue() instanceof Double || entry.getValue() instanceof Float || entry.getValue() instanceof Integer || entry.getValue() instanceof Long || entry.getValue() instanceof Boolean) {
                    valueKeys.add(entry.getKey());
                    continue;
                }
                try {
                    b.append(entry.getKey()).append("=").append(URLEncoder.encode(entry.getValue().toString(),"UTF8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                b.append(",");
            }

            b.setLength(b.length() - 1);
            b.append(" ");
            for (String v : valueKeys) {
                try {
                    b.append(v).append("=").append(URLEncoder.encode(measurement.get(v).toString(),"UTF8"));
                } catch (UnsupportedEncodingException e) {
                    e.printStackTrace();
                }
                b.append(",");
            }
            b.setLength(b.length() - 1);
            b.append(" ").append(tm);
            b.append("\n");
            measurement.put("_id", tm);
        }
        CloseableHttpClient cl = HttpClients.custom().setKeepAliveStrategy(keepAliveStrategy).
                setConnectionManager(conMgr).build();

        //need to write to all hosts in cluster
        for (String h: getHostSeed()) {
            HttpPost p = new HttpPost("http://"+h+"/write?db=" + db);

            StringEntity e = new StringEntity(b.toString(), "UTF8");
            log.info("Sending to db " + db + " on host "+h+": " + b.toString());
            p.setEntity(e);
            try {
                cl.execute(p).close();

            } catch (IOException e1) {
                throw new MorphiumDriverException("ioexception",e1);
            }
        }
    }

    public Map<String, Object> update(String s, String s1, Map<String, Object> map, Map<String, Object> map1, boolean b, boolean b1, WriteConcern writeConcern) throws MorphiumDriverException {
        log.error("Cannot run updates on influxdb");
        return null;
    }

    public Map<String, Object> delete(String db, String collection, Map<String, Object> query, boolean multiple, WriteConcern wc) throws MorphiumDriverException {
        return null;
    }

    public void drop(String db, String collection, WriteConcern wc) throws MorphiumDriverException {

    }

    public void drop(String db, WriteConcern wc) throws MorphiumDriverException {

    }

    public boolean exists(String db) throws MorphiumDriverException {
        return false;
    }

    public List<Object> distinct(String db, String collection, String field, Map<String, Object> filter, ReadPreference rp) throws MorphiumDriverException {
        return null;
    }

    public boolean exists(String db, String collection) throws MorphiumDriverException {
        return false;
    }

    public List<Map<String, Object>> getIndexes(String db, String collection) throws MorphiumDriverException {
        return null;
    }

    public List<String> getCollectionNames(String db) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> group(String db, String coll, Map<String, Object> query, Map<String, Object> initial, String jsReduce, String jsFinalize, ReadPreference rp, String... keys) throws MorphiumDriverException {
        return null;
    }

    public List<Map<String, Object>> aggregate(String db, String collection, List<Map<String, Object>> pipeline, boolean explain, boolean allowDiskUse, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    public boolean isSocketKeepAlive() {
        return false;
    }

    public void setSocketKeepAlive(boolean socketKeepAlive) {

    }

    public int getHeartbeatConnectTimeout() {
        return 0;
    }

    public void setHeartbeatConnectTimeout(int heartbeatConnectTimeout) {

    }

    public int getMaxWaitTime() {
        return 0;
    }

    public void setMaxWaitTime(int maxWaitTime) {

    }

    public boolean isCapped(String db, String coll) throws MorphiumDriverException {
        return false;
    }

    public BulkRequestContext createBulkContext(Morphium m, String db, String collection, boolean ordered, WriteConcern wc) {
        return null;
    }

    public void createIndex(String db, String collection, Map<String, Object> index, Map<String, Object> options) throws MorphiumDriverException {

    }



}
