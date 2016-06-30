package de.caluga.test.influxdbdriver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.influxdb.InfluxDbDriver;
import de.caluga.morphium.query.Query;
import org.junit.Test;

import java.util.List;
/**
 * simple Connection Test
 */

@SuppressWarnings("ALL")
public class BasicTest {

    @Test
    public void basicConnectionTest() throws Exception {
        String hosts[] = new String[]{"host_1", "host_2", "host_3"};
        String urls[] = new String[]{"/", "/checkout/new", "/search/filter"};
        String search[] = new String[]{"bohrer", "schmiermittel", "elastic", "item", "usb-hub", "Apple MacBook Pro 15"};
        String session[] = new String[]{"12345", "12333", "12328", "Session32", "123dsl", "affe", "deadbeef"};
        int ret[] = new int[]{200, 203, 301, 302, 403, 400, 500};
        MorphiumConfig cfg = new MorphiumConfig("graphite", 100, 1000, 10000);
        cfg.setDriverClass(InfluxDbDriver.class.getName());
        cfg.addHostToSeed("localhost:8086");
        cfg.setDatabase("graphite");

        cfg.setLogLevelForClass(InfluxDbDriver.class, 5);
        cfg.setGlobalLogSynced(true);
        Morphium m = new Morphium(cfg);

        for (int i = 0; i < 100; i++) {
            EntTest e = new EntTest();
            e.id = (long) (System.currentTimeMillis() * 1000 * 1000 - Math.random() * 1000 * 1000 * 1000 * 2600);


            e.host = hosts[(int) (Math.random() * hosts.length)];
            e.reqtime = 500 * Math.random() + 100 + i*10;
            e.lfd = i;
            e.success = Math.random() > 0.5;
            if (Math.random() > 0.5) {
                e.isSearch = true;
            } else {
                e.isSearch = false;
                e.search = search[(int) (Math.random() * search.length)];
            }
            e.retCode = ""+ret[(int) (Math.random() * ret.length)];
            e.sizekb = 100 * (1.0 + Math.random());
            e.url = urls[(int) (Math.random() * urls.length)];
            e.sessionid=session[(int) (session.length*Math.random())];
            if (e.url.contains("checkout")){
                e.checkoutAmount =1000000*Math.random();
            }
            m.store(e);
        }

        Query<EntTest> q=m.createQueryFor(EntTest.class).f("host").eq(hosts[0]).f("lfd").gt(12);
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
        List<EntTest> lst=q.asList();
        System.out.println("Got results:"+lst.size());

        for (EntTest et:lst){
            System.out.println("ReqTime (mean): "+et.reqtime+" - ret_code: "+et.retCode +" - lfd: "+et.lfd+" host: "+et.host);
        }

        q=m.createQueryFor(EntTest.class).f("host").eq(hosts[1]).f("lfd").gt(12).f("time()").gt("now() - 100d");
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
        lst=q.asList();
        System.out.println("Got results:"+lst.size());

        for (EntTest et:lst){
            System.out.println("ReqTime (mean): "+et.reqtime+" - ret_code: "+et.retCode +" - lfd: "+et.lfd+" host: "+et.host);
        }

        boolean gotError=false;

        try {
            q=m.createQueryFor(EntTest.class).f("host").eq(hosts[1]).f("lfd").gt(12).f("time()").gt("now(-00d");
            lst=q.asList(); //error
        } catch (Exception e) {
            System.out.println("Got expected exception!");
            e.printStackTrace();
            gotError=true;
        }
        assert(gotError);
    }

    @Entity(collectionName = "requests")
    public static class EntTest {
        @Id
        public Long id;
        public String host = "myHost";
        public String sessionid = "sessionid";
        public int lfd = 123455;
        public double reqtime = 123.345;
        public boolean success = true;
        public String url = "";
        public String retCode = "404";
        public double sizekb = 0;
        public String search = "";
        public boolean isSearch = false;
        public double checkoutAmount =0;


    }
}
