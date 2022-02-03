# Qualise proxy for Enedis SGE Tiers

Access energy consumption data from Enedis through the Quoalise protocol.

## Command line usage

Data can be accessed with the [quoalise](https://github.com/consometers/quoalise) client.

## Available data

### Load Curves

- `urn:dev:prm:<PRM>_consumption/active_power/raw`
- `urn:dev:prm:<PRM>_consumption/active_power/corrected`
- `urn:dev:prm:<PRM>_production/active_power/raw`
- `urn:dev:prm:<PRM>_production/active_power/corrected`

Mean active power consumed over a period of time (10 min., 30 min., 60 min. depending on the counter). Mean value for each time period is time stamped at the begining.

Example:

```bash
quoalise get-records sge-proxy@xmpp-provider.io/proxy urn:dev:prm:14411643921305_consumption/active_power/raw --start-date 2021-12-01 --end-date 2021-12-05
```

First returned value will be 342 W, stamped at 2021-11-30T23:00:00 UTC. Mean consumed power have been 342 W between 00:00 and 00:30 on the 2021-12-01 (french time).

Last returned value will be stamped at 2021-12-05T22:30:00 UTC.

Limitations:

- Trottled to one request per second
- Maximum 7 days requested at once
- Oldest data 36 months ago