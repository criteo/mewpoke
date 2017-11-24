# MEWPOKE - Hope of a mew probe

<p align="center">
  <img src="./mewpoke_logo.png" alt="logo"/>
</p>


This project contains a probe aiming to monitor latencies and avaibilities for Couchbase and Memcached nodes.

Metrics gathered are exposed through a prometheus endpoint available at ```Config:app:httpServerPort```/metrics

### Build
1. Clone the repository
2. Ensure you have Java 8 and [gradle](https://gradle.org/install/) installed
3. Run `gradle build` at the root of the directory
4. Artifacts can be founds inside the `build/libs` directory

### Run
1. Ensure you have a valid config file (see [configuration](./CONFIGURATION.md)) 
2. Run `java -jar my_uber_jar.jar my_config.yml`
3. That's all, Enjoy !!

