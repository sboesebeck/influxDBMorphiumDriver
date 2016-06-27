# InfluxDB Driver for Morphium 3.x

This is an implementation of the `MorphiumDriver` interface in the [morphium](https://github.com/sboesebeck/morphium) project. 

Morphium was built to be a mongodb abstraction layer and driver. With Version `3.0` a new Driver architecture was introduced, making morphium able to work with theoretically any kind of database.

InfluxDB is a time series db which is something very different to mongodb and does not have a complex ql like a full blown SQL-DB.

Of course, using this driver will have some drawbacks, you will not be able to do everything, you could do with the CLI or a native driver.

## Usecases

- 