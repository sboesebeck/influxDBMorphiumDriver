package de.caluga.morphium.influxdb;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpEntityEnclosingRequestBase;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * basic influx db support for morphium
 * <p>
 * translation of the morphium fluent interface for queries to influxQL has some caveats:
 * <p>
 * <ul>
 * <li>not possible to issue several select commands in one query</li>
 * <li>if you want to use aggregation, add the aggregate function to the projection like <code>query.addProjection("reqtime","mean");</code></li>
 * <li>complex queries rely on setting the query to the key <code>qstr</code> (for query string). Make sure this is valid!</li>
 * </ul>
 */
public class InfluxDbDriver implements MorphiumDriver {
    private final Logger log = new Logger(InfluxDbDriver.class);
    private final ConnectionKeepAliveStrategy keepAliveStrategy = new ConnectionKeepAliveStrategy() {
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
    private int maxConPerHost = 1;
    private int socketTimeout = 0;
    private int conTimeout = 5000;
    private PoolingHttpClientConnectionManager conMgr;
    private String[] hosts;
    private String login;
    private String password;
    private int sleepBetweenNetworkErrorRetries = 2000;
    private int networkRetries = 1;
    private ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1);

    private Map<String, List<HttpEntityEnclosingRequestBase>> failedRequests = new HashMap<String, List<HttpEntityEnclosingRequestBase>>();

    public void setCredentials(String db, String login, char[] pwd) {
        this.login = login;
        this.password = new String(pwd);
    }

    public boolean isReplicaset() {
        return hosts != null && hosts.length > 0;
    }

    public String[] getCredentials(String db) {
        return new String[]{login, password};
    }

    public boolean isDefaultFsync() {
        return false;
    }

    public void setDefaultFsync(boolean j) {

    }

    public String[] getHostSeed() {
        return hosts;
    }

    public void setHostSeed(String... host) {
        hosts = host;
    }

    public int getMaxConnectionsPerHost() {
        return maxConPerHost;
    }

    public void setMaxConnectionsPerHost(int mx) {
        maxConPerHost = mx;
    }

    public int getMinConnectionsPerHost() {
        return 1;
    }

    public void setMinConnectionsPerHost(int mx) {

    }

    public int getMaxConnectionLifetime() {
        return conTimeout;
    }

    public void setMaxConnectionLifetime(int timeout) {
        conTimeout = timeout;
    }

    public int getMaxConnectionIdleTime() {
        return conTimeout;
    }

    public void setMaxConnectionIdleTime(int time) {
        conTimeout = time;
    }

    public int getSocketTimeout() {
        return socketTimeout;
    }

    public void setSocketTimeout(int timeout) {
        socketTimeout = timeout;
    }

    public int getConnectionTimeout() {
        return conTimeout;
    }

    public void setConnectionTimeout(int timeout) {
        conTimeout = timeout;
    }

    public int getDefaultW() {
        return 0;
    }

    public void setDefaultW(int w) {

    }

    public int getMaxBlockintThreadMultiplier() {
        return 1;
    }

    public int getHeartbeatFrequency() {
        return 1000;
    }

    public void setHeartbeatFrequency(int heartbeatFrequency) {

    }

    public void setDefaultBatchSize(int defaultBatchSize) {

    }

    public void setCredentials(Map<String, String[]> credentials) {

    }

    public int getHeartbeatSocketTimeout() {
        return 0;
    }

    public void setHeartbeatSocketTimeout(int heartbeatSocketTimeout) {

    }

    public boolean isUseSSL() {
        return false;
    }

    public void setUseSSL(boolean useSSL) {
        if (useSSL) {
            throw new IllegalArgumentException("SSL not supported");
        }

    }

    public boolean isDefaultJ() {
        return false;
    }

    public void setDefaultJ(boolean j) {

    }

    public int getWriteTimeout() {
        return 0;
    }

    public void setWriteTimeout(int writeTimeout) {

    }

    public int getLocalThreshold() {
        return 0;
    }

    public void setLocalThreshold(int thr) {

    }

    public void setMaxBlockingThreadMultiplier(int m) {

    }

    public void heartBeatFrequency(int t) {

    }

    public void heartBeatSocketTimeout(int t) {

    }

    public void useSsl(boolean ssl) {
        if (ssl) {
            throw new IllegalArgumentException("SSL not supported");
        }
    }

    public void connect() throws MorphiumDriverException {
        conMgr = new PoolingHttpClientConnectionManager(getMaxConnectionLifetime(), TimeUnit.MILLISECONDS);
        conMgr.setDefaultMaxPerRoute(100);
        conMgr.setMaxTotal(100000);


    }

    public void setDefaultReadPreference(ReadPreference rp) {

    }

    public void connect(String replicasetName) throws MorphiumDriverException {
        connect();
    }

    public Maximums getMaximums() {
        Maximums ret = new Maximums();
        ret.setMaxBsonSize(Integer.MAX_VALUE);
        ret.setMaxMessageSize(Integer.MAX_VALUE);
        ret.setMaxWriteBatchSize(100);
        return ret;
    }

    public boolean isConnected() {
        return conMgr != null;
    }

    public int getDefaultWriteTimeout() {
        return 0;
    }

    public void setDefaultWriteTimeout(int wt) {

    }

    public int getRetriesOnNetworkError() {
        return networkRetries;
    }

    public void setRetriesOnNetworkError(int r) {
        networkRetries = r;
    }

    public int getSleepBetweenErrorRetries() {
        return sleepBetweenNetworkErrorRetries;
    }

    public void setSleepBetweenErrorRetries(int s) {
        sleepBetweenNetworkErrorRetries = s;
    }

    public void close() throws MorphiumDriverException {
        conMgr.close();
        conMgr = null;
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
        try {
            CloseableHttpResponse resp = doRequest(db, "query", cmd.get("qstr").toString());
            BufferedReader in = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
            JSONParser parser = new JSONParser();
            @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) parser.parse(in);
            resp.close();
            return result;
        } catch (IOException e) {
            throw new MorphiumDriverException("IO Error", e);
        } catch (ParseException e) {
            throw new MorphiumDriverException("Parse error", e);
        }
    }

    private CloseableHttpResponse doRequest(String db, String op, String str) throws MorphiumDriverException {
        int count = 0;
        while (true) {
            count++;
            CloseableHttpClient cl = HttpClients.custom().setKeepAliveStrategy(keepAliveStrategy).
                    setConnectionManager(conMgr).build();

            String h = getHostSeed()[(int) (Math.random() * getHostSeed().length)];
            HttpGet p = null;
            StringBuilder auth = new StringBuilder();
            if (login != null) {
                try {
                    auth.append("&u=").append(URLEncoder.encode(login, "UTF8"));
                    auth.append("&p=").append(URLEncoder.encode(password, "UTF8"));
                } catch (UnsupportedEncodingException e) {
                    log.error("Authentication failed!", e);
                }
            }
            try {
                p = new HttpGet("http://" + h + "/" + op + "?db=" + db + auth.toString() + "&q=" + URLEncoder.encode(str, "UTF8"));
                CloseableHttpResponse resp;
                resp = cl.execute(p);
                return resp;
            } catch (Exception e) {
                if (count > getRetriesOnNetworkError()) {
                    throw new MorphiumDriverException("io exception", e);
                }
                //otherwise retry
                try {
                    Thread.sleep(getSleepBetweenErrorRetries());
                } catch (InterruptedException ignored) {
                    log.debug("Sleep interrupted", ignored);
                }
            }
        }
    }

    public MorphiumCursor initIteration(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference readPreference, Map<String, Object> findMetaData) throws MorphiumDriverException {
        MorphiumCursor<InfluxCursor> crs = new MorphiumCursor<InfluxCursor>();

        crs.setBatch(find(db, collection, query, sort, projection, skip, limit, batchSize, readPreference, findMetaData));
        crs.setCursorId(System.currentTimeMillis());
        InfluxCursor ic = new InfluxCursor();
        ic.db = db;
        ic.collection = collection;
        ic.query = query;
        ic.sort = sort;
        ic.projection = projection;
        ic.skip = skip + limit;
        ic.limit = limit;
        crs.setInternalCursorObject(ic);
        return crs;
    }

    public MorphiumCursor nextIteration(MorphiumCursor crs) throws MorphiumDriverException {
        InfluxCursor ic = (InfluxCursor) crs.getInternalCursorObject();
        crs.setBatch(find(ic.db, ic.collection, ic.query, ic.sort, ic.projection, ic.skip, ic.limit, 0, null, null));
        ic.skip = ic.skip + ic.limit;
        return crs;
    }

    public void closeIteration(MorphiumCursor crs) throws MorphiumDriverException {
        //nothing to do here!
    }

    public List<Map<String, Object>> find(String db, String collection, Map<String, Object> query, Map<String, Integer> sort, Map<String, Object> projection, int skip, int limit, int batchSize, ReadPreference rp, Map<String, Object> findMetaData) throws MorphiumDriverException {
        StringBuilder b = new StringBuilder();
        b.append("select ");
        StringBuilder groupBy = new StringBuilder();
        for (Map.Entry<String, Object> m : projection.entrySet()) {
            if (!m.getValue().equals(0)) {
                if (m.getKey().equals("_id")) {
                    continue;
                }
                if (m.getValue().equals(1)) {
                    b.append(m.getKey());
                } else {
                    if (m.getValue().toString().equalsIgnoreCase("group by")) {
                        groupBy.append(m.getKey());
                        continue;
                    }
                    b.append(m.getValue()).append("(");
                    b.append(m.getKey()).append(")");
                    b.append(" as ").append(m.getKey());
                }
                b.append(",");
            }

        }
        b.setLength(b.length() - 1);
        if (b.toString().equals("select ")) {
            b.append("*");
        }
        b.append(" from ").append(collection);
        //where clause
        if (!query.isEmpty()) {
            b.append(" where ");
            addQueryString(b, query);
        }
        if (groupBy.length() != 0) {
            b.append(" group by ");
            b.append(groupBy);
        }
        if (skip > 0) {
            b.append(" offset ").append(skip);
        }
        if (limit > 0) {
            b.append(" limit ").append(limit);
        }
        log.info("Query " + b.toString());

        //            log.info("Sending to db " + db + " on host " + h + ": " + b.toString());
        List<Map<String, Object>> res = new ArrayList<Map<String, Object>>();
        try {
            CloseableHttpResponse resp = doRequest(db, "query", b.toString());
            BufferedReader in = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()));
            JSONParser parser = new JSONParser();
            @SuppressWarnings("unchecked") Map<String, Object> result = (Map<String, Object>) parser.parse(in);
            resp.close();
            log.info("Got Result!");
            if (result.get("error") != null) {
                throw new MorphiumDriverException(result.get("error").toString());
            }
            if (result.get("results") == null) {
                return res;
            }
            JSONArray series = (JSONArray) ((JSONObject) ((JSONArray) result.get("results")).get(0)).get("series");

            for (Object o : series) {
                JSONObject obj = (JSONObject) o;
                Map<String, Object> resObj = new HashMap<String, Object>();
                JSONArray cols = (JSONArray) obj.get("columns");
                JSONArray values = ((JSONArray) obj.get("values"));
                JSONObject tags = (JSONObject) obj.get("tags");

                for (Object val : values) {
                    JSONArray arr = (JSONArray) val;

                    for (int i = 0; i < cols.size(); i++) {
                        if (cols.get(i).equals("time")) {
                            try {
                                resObj.put("_id", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").parse(arr.get(i).toString()).getTime());
                            } catch (java.text.ParseException e) {
                                try {
                                    resObj.put("_id", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").parse(arr.get(i).toString()).getTime());
                                } catch (java.text.ParseException e1) {
                                    e1.printStackTrace();
                                }
                            }
                        } else {

                            resObj.put(cols.get(i).toString(), arr.get(i));
                        }
                    }
                    //noinspection unchecked
                    resObj.putAll(tags);
                    res.add(resObj);
                }
            }

        } catch (IOException e1) {
            throw new MorphiumDriverException("ioexception", e1);
        } catch (ParseException e1) {
            e1.printStackTrace();
        }

        return res;
    }


    private void addQueryString(StringBuilder b, Map<String, Object> query) {
        for (Map.Entry<String, Object> e : query.entrySet()) {
            if (e.getKey().equals("_id")) {
                //this is the timestamp...
                continue;
            }
            boolean time = false;
            if (e.getKey().equals("$and") || e.getKey().equals("$or")) {
                //And concatenation
                @SuppressWarnings("unchecked") List<Map<String, Object>> subQueries = (List<Map<String, Object>>) e.getValue();
                for (Map<String, Object> q : subQueries) {
                    addQueryString(b, q);
                    if (e.getKey().equals("$and")) {
                        b.append(" AND ");
                    } else {
                        b.append(" OR ");
                    }
                }
                trimLastBooleanOp(b);
                continue;
            } else if (e.getKey().equals("time()")) {
                b.append("time");
                time = true;
            } else {
                b.append(e.getKey());
            }
            if (e.getValue() instanceof Map) {
                @SuppressWarnings("unchecked") Map<String, Object> qe = (Map<String, Object>) e.getValue();
                for (Map.Entry<String, Object> en : qe.entrySet()) {
                    if (en.getKey().equals("$eq")) {
                        b.append("=");
                    } else if (en.getKey().equals("$match")) {
                        log.error("Pattern matching");
                    } else if (e.getKey().equals("$ne")) {
                        b.append("!=");
                    } else if (en.getKey().equals("$gt")) {
                        b.append(">");
                    } else if (en.getKey().equals("$gte")) {
                        b.append(">=");
                    } else if (en.getKey().equals("$lte")) {
                        b.append("<=");
                    } else if (en.getKey().equals("$lt")) {
                        b.append("<");
                    } else {
                        throw new RuntimeException("Unsupported operand " + e.getValue() + " for field " + e.getKey());
                    }
                    if (en.getValue() instanceof String && !time) {
                        b.append("'").append(en.getValue()).append("'");
                    } else {
                        b.append(en.getValue());
                    }
                }
            } else {

                b.append("=");
                if (e.getValue() instanceof String && !time) {
                    b.append("'").append(e.getValue()).append("'");
                } else {
                    b.append(e.getValue());
                }
                b.append(" AND ");
            }
        }
        trimLastBooleanOp(b);
    }

    private void trimLastBooleanOp(StringBuilder b) {
        if (b.toString().endsWith(" AND ")) {
            b.setLength(b.length() - 5);
        } else if (b.toString().endsWith(" OR ")) {
            b.setLength(b.length() - 4);
        }
    }

    public long count(String db, String collection, Map<String, Object> query, ReadPreference rp) throws MorphiumDriverException {
        return 0;
    }

    public void insert(String db, String collection, List<Map<String, Object>> objs, WriteConcern wc) throws MorphiumDriverException {
        store(db, collection, objs, wc);
    }

    public void store(String db, String collection, List<Map<String, Object>> list, WriteConcern writeConcern) throws MorphiumDriverException {

        StringBuilder b = new StringBuilder();
        b.append(collection).append(",");

        //assuming number and boolean values as influx values
        //all others as tags

        for (Map<String, Object> measurement : list) {
            long tm = System.currentTimeMillis()*1000;

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

                if (entry.getKey().equals("_id")) {
                    continue;
                }
                if (entry.getValue() instanceof Number || entry.getValue() instanceof Double || entry.getValue() instanceof Float || entry.getValue() instanceof Integer || entry.getValue() instanceof Long || entry.getValue() instanceof Boolean) {
                    valueKeys.add(entry.getKey());
                    continue;
                }
                b.append(entry.getKey()).append("=").append(entry.getValue());
                b.append(",");
            }

            b.setLength(b.length() - 1);
            b.append(" ");
            for (String v : valueKeys) {
                b.append(v).append("=").append(measurement.get(v));
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
        for (String h : getHostSeed()) {
            int retryCount = 0;
            while (true) {
                retryCount++;
                HttpPost p = new HttpPost("http://" + h + "/write?db=" + db);

                StringEntity e = new StringEntity(b.toString(), "UTF8");
                //            log.info("Sending to db " + db + " on host " + h + ": " + b.toString());
                p.setEntity(e);
                try {
                    cl.execute(p).close();

                } catch (IOException e1) {
                    if (retryCount > getRetriesOnNetworkError()) {
                        List<String> hlst = Arrays.asList(hosts);
                        hlst.remove(h);
                        hosts = hlst.toArray(hosts);
                        failedRequests.putIfAbsent(h, new ArrayList<HttpEntityEnclosingRequestBase>());
                        failedRequests.get(h).add(p);
                        throw new MorphiumDriverException("could not write to host " + h + " - possible data loss!!!!", e1);
                    }
                }
                try {
                    Thread.sleep(getSleepBetweenErrorRetries());
                } catch (InterruptedException e2) {
                    log.debug("sleep interrupted", e2);
                }
                break;
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


    private class InfluxCursor {
        public int skip = 0;
        public int limit = 0;
        public String db;
        public String collection;
        public Map<String, Object> query;
        public Map<String, Integer> sort;
        public Map<String, Object> projection;
    }

}
