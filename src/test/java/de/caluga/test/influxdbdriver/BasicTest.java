package de.caluga.test.influxdbdriver;

import de.caluga.morphium.Morphium;
import de.caluga.morphium.MorphiumConfig;
import de.caluga.morphium.annotations.Entity;
import de.caluga.morphium.annotations.Id;
import de.caluga.morphium.influxdb.InfluxDbDriver;
import org.junit.Test;

/**
 * Created by stephan on 22.06.16.
 */

public class BasicTest {

    @Test
    public void basicConnectionTest() throws Exception{
        MorphiumConfig cfg=new MorphiumConfig("graphite",100,1000,10000);
        cfg.setDriverClass(InfluxDbDriver.class.getName());
        cfg.addHostToSeed("localhost:8086");
        Morphium m=new Morphium(cfg);

        m.store(new EntTest());
    }

    @Entity
    public static class EntTest {
        @Id
        private String id;
        private double value=123.345;
    }
}
