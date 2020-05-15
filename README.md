[![Build Status](https://travis-ci.org/criteo/mewpoke.svg?branch=master)](https://travis-ci.org/criteo/mewpoke)

Project archived, due to the number of ad hoc features/behavior needed for our custom requirements. So it is no longer worth putting it as open source. The project is still working.

# MEWPOKE - Hope of a mew probe

<p align="center">
  <img src="https://github.com/gfediere/mewpoke-environment/blob/master/mewpoke_logo.png" alt="logo"/>
</p>

## Overview

This project contains a probe aiming to monitor latencies and avaibilities for Couchbase and Memcached nodes.

The targeted clusters can either be discovered through Consul, or via a node (other nodes and buckets would then me discoverred automatically)

Metrics are gathered periodically and exposed through a prometheus endpoint available at ```Config:app:httpServerPort```/metrics

## Exposed metrics

| Metric | Description | Labels
| --- | --- | --- |
| memcached_up | Availability of a given bucket on a given memcached node | cluster, bucket, instance |
| memcached_latency | Get and Set latencies | cluster, bucket, instance, command, quantile |
| memcached_stats | Memcached global stats (ex: total_items, get_misses,..etc.) | cluster, bucket, instance, name |
| memcached_items  | Memcached slab specific metrics (ex: moves_to_warm, evicted,..etc.) | cluster, bucket, instance, slabsizerange, name |
| couchbase_up | Availability of a given bucket on a given Couchbase node | cluster, bucket, instance |
| couchbase_latency | Disk persistence latency for a set operation | cluster, bucket, instance, command, quantile |
| couchbase_operations | Ongoing rebalance operation on the cluster | cluster, operation |
| couchbase_stats | Couchbase specific stats that are listed in the config file in the ```couchbaseStats/bucket``` section | cluster, bucket, instance, name |
| couchbase_xdcr | Couchbase replication stats that are listed in the config file in the ```couchbaseStats/xdcr``` section | cluster, bucket, remotecluster, name |
| couchbase_membership | Cluster membership status | cluster, membership |


## Build

1. Clone the repository
2. Ensure you have Java 8 and [gradle](https://gradle.org/install/) installed
3. Run `gradle build` at the root of the directory
4. Artifacts can be founds inside the `build/libs` directory

## Run

1. Ensure you have a valid config file (see [configuration](./CONFIGURATION.md)) 
2. Run `java -jar my_uber_jar.jar my_config.yml`
3. That's all, Enjoy !!

