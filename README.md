hjq: jq for Hadoop MapReduce
============================

hjq let's you run jq in Hadoop MapReduce.

Map-only example:
```bash
hjq --input /PATH/TO/INPUT --output /PATH/TO/OUTPUT --mapper '{name, size: .wins | length}
```

If you use a reducer, your mapper must spit out objects with "key" and "value" fields:
```bash
hjq --input /PATH/TO/INPUT --output /PATH/TO/OUTPUT --mapper '{key: .name, value: .wins}'  --reducer '{name: .[0].key, len: .[] | length}'`
```

Building
========



Extended Example
================
