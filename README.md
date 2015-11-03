hjq: jq for Hadoop MapReduce
============================

hjq let's you run jq in Hadoop MapReduce.

Map-only example:
`hjq --input input --output output --mapper '{name, size: .wins | length}`

If you use a reducer, your mapper must spit out objects with "key" and "value" fields:
`hjq --input input --output output --mapper '{key: .name, value: .wins}'  --reducer '{name: .[0].key, len: .[] | length}'`
