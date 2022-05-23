# Qualise proxy for Enedis SGE Tiers

Access energy consumption data from Enedis through the Quoalise protocol.

## Command line usage

Data can be accessed with the [quoalise](https://github.com/consometers/quoalise) client.

## Available data

### Load Curves history

- `urn:dev:prm:<PRM>_consumption/power/active/raw`
- `urn:dev:prm:<PRM>_consumption/power/active/corrected`
- `urn:dev:prm:<PRM>_production/power/active/raw`
- `urn:dev:prm:<PRM>_production/power/active/corrected`

Mean active power consumed over a period of time (10 min., 30 min., 60 min. depending on the counter). Mean value for each time period is time stamped at the begining.

Example:

```bash
quoalise get-history sge-proxy@xmpp-provider.io/proxy urn:dev:prm:14411643921305_consumption/power/active/raw --start-date 2021-12-01 --end-date 2021-12-05
```

First returned value will be 342 W, stamped at 2021-11-30T23:00:00 UTC. Mean consumed power have been 342 W between 00:00 and 00:30 on the 2021-12-01 (french time).

Last returned value will be stamped at 2021-12-05T22:30:00 UTC.

Limitations:

- Trottled to one request per second
- Maximum 7 days requested at once
- Oldest data 36 months ago, limited by the latest commissioning

### Daily energy history

- `urn:dev:prm:<PRM>_consumption/energy/active/daily`
- `urn:dev:prm:<PRM>_production/energy/active/daily`

Limitations:

- Available for Linky devices only (C5 and P4 segments)
- Oldest data 36 months ago, limited by the latest commissioning

## Contributing

Please run black and flake8 before commit. It can be done automatically with a git pre-commit hook:

```bash
pre-commit install
```