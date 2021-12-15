# Qualise proxy for Enedis SGE Tiers

Access energy consumption data from Enedis through the Quoalise protocol.

## Command line usage

Data can be accessed with the [quoalise](https://github.com/consometers/quoalise) client.

```bash
quoalise get-records sge-proxy@xmpp-provider.io/proxy urn:dev:prm:30001610071843_consumption/active_power/raw --start-date 2021-12-01 --end-date 2021-12-05
```

## Available data

### Measurements

- `urn:dev:prm:<PRM>_consumption/active_power/raw`
- `urn:dev:prm:<PRM>_consumption/active_power/corrected`
- `urn:dev:prm:<PRM>_production/active_power/raw`
- `urn:dev:prm:<PRM>_production/active_power/corrected`

Limitations:

- Trottled to one request per second
- Maximum 7 days requested at once
- Oldest data 36 months ago