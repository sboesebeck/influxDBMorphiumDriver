package de.caluga.morphium.influxdb;

import de.caluga.morphium.Logger;
import de.caluga.morphium.Morphium;
import de.caluga.morphium.driver.*;
import de.caluga.morphium.driver.bulk.BulkRequestContext;
import de.caluga.morphium.driver.mongodb.Maximums;
import org.apache.http.HttpClientConnection;
import org.apache.http.HttpEntity;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
    private PoolingHttpClientConnectionManager conMgr=new PoolingHttpClientConnectionManager(getMaxConnectionLifetime(), TimeUnit.MILLISECONDS);

    public void setCredentials(String s, String s1, char[] chars) {

    }

    public boolean isReplicaset() {
        return false;
    }

    public String[] getCredentials(String s) {
        return new String[0];
    }

    public boolean isDefaultFsync() {
        return false;
    }

    public String[] getHostSeed() {
        return new String[0];
    }

    public int getMaxConnectionsPerHost() {
        return 0;
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

    public void setHeartbeatSocketTimeout(int i) {

    }

    public void setUseSSL(boolean b) {

    }

    public void setHeartbeatFrequency(int i) {

    }

    public void setWriteTimeout(int i) {

    }

    public void setDefaultBatchSize(int i) {

    }

    public void setCredentials(Map<String, String[]> map) {

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

    public void setHostSeed(String... strings) {

    }

    public void setMaxConnectionsPerHost(int i) {

    }

    public void setMinConnectionsPerHost(int i) {

    }

    public void setMaxConnectionLifetime(int i) {

    }

    public void setMaxConnectionIdleTime(int i) {

    }

    public void setSocketTimeout(int i) {

    }

    public void setConnectionTimeout(int i) {

    }

    public void setDefaultW(int i) {

    }

    public void setMaxBlockingThreadMultiplier(int i) {

    }

    public void heartBeatFrequency(int i) {

    }

    public void heartBeatSocketTimeout(int i) {

    }

    public void useSsl(boolean b) {

    }

    public void connect() throws MorphiumDriverException {

    }

    public void setDefaultReadPreference(ReadPreference readPreference) {

    }

    public void connect(String s) throws MorphiumDriverException {
        //replicaset is ignored


    }

    public Maximums getMaximums() {
        return null;
    }

    public boolean isConnected() {
        return false;
    }

    public void setDefaultJ(boolean b) {

    }

    public void setDefaultWriteTimeout(int i) {

    }

    public int getDefaultWriteTimeout() {
        return 0;
    }

    public void setLocalThreshold(int i) {

    }

    public void setDefaultFsync(boolean b) {

    }

    public void setRetriesOnNetworkError(int i) {

    }

    public int getRetriesOnNetworkError() {
        return 0;
    }

    public void setSleepBetweenErrorRetries(int i) {

    }

    public int getSleepBetweenErrorRetries() {
        return 0;
    }

    public void close() throws MorphiumDriverException {

    }

    public Map<String, Object> getReplsetStatus() throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> getDBStats(String s) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> getOps(long l) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> runCommand(String s, Map<String, Object> map) throws MorphiumDriverException {
        return null;
    }

    public MorphiumCursor initIteration(String s, String s1, Map<String, Object> map, Map<String, Integer> map1, Map<String, Object> map2, int i, int i1, int i2, ReadPreference readPreference, Map<String, Object> map3) throws MorphiumDriverException {
        return null;
    }

    public MorphiumCursor nextIteration(MorphiumCursor morphiumCursor) throws MorphiumDriverException {
        return null;
    }

    public void closeIteration(MorphiumCursor morphiumCursor) throws MorphiumDriverException {

    }

    public List<Map<String, Object>> find(String s, String s1, Map<String, Object> map, Map<String, Integer> map1, Map<String, Object> map2, int i, int i1, int i2, ReadPreference readPreference, Map<String, Object> map3) throws MorphiumDriverException {
        return null;
    }

    public long count(String s, String s1, Map<String, Object> map, ReadPreference readPreference) throws MorphiumDriverException {
        return 0;
    }

    public void insert(String s, String s1, List<Map<String, Object>> list, WriteConcern writeConcern) throws MorphiumDriverException {

    }

    public void store(String s, String s1, List<Map<String, Object>> list, WriteConcern writeConcern) throws MorphiumDriverException {
        CloseableHttpClient cl= HttpClients.createDefault();
        HttpPost p=new HttpPost("http://localhost:8086/write?db=graphite");
        StringEntity e=new StringEntity("cpu_load_short,host=server01,region=us-west value=0.64 "+System.currentTimeMillis(),"UTF8");
        p.setEntity(e);
        try {
            cl.execute(p);
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public Map<String, Object> update(String s, String s1, Map<String, Object> map, Map<String, Object> map1, boolean b, boolean b1, WriteConcern writeConcern) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> delete(String s, String s1, Map<String, Object> map, boolean b, WriteConcern writeConcern) throws MorphiumDriverException {
        return null;
    }

    public void drop(String s, String s1, WriteConcern writeConcern) throws MorphiumDriverException {

    }

    public void drop(String s, WriteConcern writeConcern) throws MorphiumDriverException {

    }

    public boolean exists(String s) throws MorphiumDriverException {
        return false;
    }

    public List<Object> distinct(String s, String s1, String s2, Map<String, Object> map, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    public boolean exists(String s, String s1) throws MorphiumDriverException {
        return false;
    }

    public List<Map<String, Object>> getIndexes(String s, String s1) throws MorphiumDriverException {
        return null;
    }

    public List<String> getCollectionNames(String s) throws MorphiumDriverException {
        return null;
    }

    public Map<String, Object> group(String s, String s1, Map<String, Object> map, Map<String, Object> map1, String s2, String s3, ReadPreference readPreference, String... strings) throws MorphiumDriverException {
        return null;
    }

    public List<Map<String, Object>> aggregate(String s, String s1, List<Map<String, Object>> list, boolean b, boolean b1, ReadPreference readPreference) throws MorphiumDriverException {
        return null;
    }

    public boolean isSocketKeepAlive() {
        return false;
    }

    public void setSocketKeepAlive(boolean b) {

    }

    public int getHeartbeatConnectTimeout() {
        return 0;
    }

    public void setHeartbeatConnectTimeout(int i) {

    }

    public int getMaxWaitTime() {
        return 0;
    }

    public void setMaxWaitTime(int i) {

    }

    public boolean isCapped(String s, String s1) throws MorphiumDriverException {
        return false;
    }

    public BulkRequestContext createBulkContext(Morphium morphium, String s, String s1, boolean b, WriteConcern writeConcern) {
        return null;
    }

    public void createIndex(String s, String s1, Map<String, Object> map, Map<String, Object> map1) throws MorphiumDriverException {

    }
}
