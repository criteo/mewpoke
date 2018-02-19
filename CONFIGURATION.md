# Configuration

Mewpoke get all infos from his configuration's file ```config.yml```

# Sections

## app
- `measurementPeriodInSec`: time between metrics refresh
- `refreshDiscoveryPeriodInSec`: time between discovery refresh
- `httpServerPort`: port used to expose metrics

## discovery
- `consul`: use consul as endpoint for discovery
 -- *host*: consul endpoint targeted
 -- *port*: consul port
 -- *timeoutInSec*: timeout for consul requests
 -- *readConsistency*: define consul consistency mode (see [Consistency-modes] (https://www.consul.io/api/index.html#consistency-modes) for more infos)
 -- *tags*: list of tags for consul parsing
- `staticDns` use static host as endpoint for discovery
 -- *clustername*: name of the cluster
 -- *host*: hostname
 -- *bucketpassword*: bucket's password specified in the "Access Control" part of Bucket settings (all buckets within the cluster should have the same password / this feature will disable latency metrics)

## service
- `type`:

| Service | Metrics provided |
| ------ | ------ |
| COUCHBASE_STATS | Couchbase metrics from API: ([buckets by node](https://developer.couchbase.com/documentation/server/3.x/admin/REST/rest-bucket-stats.html) and [XDCR stats](https://developer.couchbase.com/documentation/server/3.x/admin/REST/rest-xdcr-statistics.html)) |
| COUCHBASE | return metrics Latency to persist Data on disk and process up |
| MEMCACHED | return metrics Lateny on buckets by node|
- `timeoutInSec`: timeout for service requests
- `username`: Couchbase username
- `password`: Couchbase password

## couchbaseStats
- `bucket`: list all bucket stats exported (if empty, will export all metrics)
- `xdcr`: list all xdcr stats exported (if empty, will export all metrics)

# Example

```yaml
app:
  tickRateInSec: 20
  httpServerPort: 8000

discovery:
  refreshEveryMin: 1
  consul:
    host: consul01.domain.local
    port: 8500
    timeoutInSec: 10
    readConsistency: STALE
    refreshEveryMin: 1
    tags:
      - cluster=couchbase01

service:
  type:  COUCHBASE_STATS
  timeoutInSec: 60
  username: Administrator
  password: password

couchbaseStats:
    bucket:
    xdcr:
```

