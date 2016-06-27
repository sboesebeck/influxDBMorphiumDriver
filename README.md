# InfluxDB Driver for Morphium 3.x

This is an implementation of the `MorphiumDriver` interface in the [morphium](https://github.com/sboesebeck/morphium) project. 

Morphium was built to be a mongodb abstraction layer and driver. With Version `3.0` a new Driver architecture was introduced, making morphium able to work with theoretically any kind of database.

InfluxDB is a time series db which is something very different to mongodb and does not have a complex ql like a full blown SQL-DB.

Of course, using this driver will have some drawbacks, you will not be able to do everything, you could do with the CLI or a native driver.

## Usecases

- send timeseries data to influxdb in an efficient and "known" way utelizing all the good stuff, morphium does offer (bulk requests etc)
- simple cluster support
- do simple queries to influx, gathering high level information

what this driver will __not__ do:

- help you visualizing things
- administration of influxdb


## How To

quite simple. Instanciate Morphium as usual, just set a different driver:

```java
        MorphiumConfig cfg = new MorphiumConfig("graphite", 100, 1000, 10000);
        cfg.setDriverClass(InfluxDbDriver.class.getName());
        cfg.addHostToSeed("localhost:8086");
        cfg.setDatabase("graphite");

        cfg.setLogLevelForClass(InfluxDbDriver.class, 5);
        cfg.setGlobalLogSynced(true);
        Morphium m = new Morphium(cfg);
```

Sending Data do influx works same as if it was a mongodb:

```java
            EntTest e = new EntTest();
            //fill with data ...            
            m.store(e);
```

reading from influx works also similar to mongodb, with minor changes:

```java
  Query<EntTest> q=m.createQueryFor(EntTest.class).f("host").eq(hosts[0]).f("lfd").gt(12);
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
        List<EntTest> lst=q.asList();
        System.out.println("Got results:"+lst.size());
```

you need to set the aggregation method as projection operator. Also the `group by` clause is modeled ther.

And one major _hack_: if you want to do queries using time constraints like `where time > now() - 10d` you need to query using `time()`as morphium needs to detect that this is not a field but a function you're calling:

```java
 q=m.createQueryFor(EntTest.class).f("host").eq(hosts[1]).f("lfd").gt(12).f("time()").gt("now() - 100d");
        q.addProjection("reqtime","mean");
        q.addProjection("ret_code","group by");
```
