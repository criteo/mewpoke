# Configuration

Mewpoke get all infos from his configuration's file ```config.yml```

# Sections

## app
- `tickRateInSec`: time between metrics refresh
- `httpServerPort`: port used to expose metrics

## discovery
- `refreshEveryMin`: time between discovery refresh
- `consul`: use consul as endpoint for discovery
 -- *host*: consul endpoint targeted
 -- *port*: consul port
 -- *timeoutInSec*: timeout for consul requests
 -- *readConsistency*: define consul consistency mode (see [Consistency-modes] (https://www.consul.io/api/index.html#consistency-modes) for more infos)
 -- *tags*: list of tags for consul parsing
- `staticDns` use static host as endpoint for discovery
 -- *clustername*: name of the cluster
 -- *host*: hostname

## service
- `type`: metric requested (only COUCHBASE_STATS available now)
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

