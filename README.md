hjq: jq for Hadoop MapReduce
============================

hjq lets you run [jq](https://stedolan.github.io/jq/) programs across large volumes of JSON and Avro data using Hadoop MapReduce.

Consider this sample poker game dataset from [the JSON Lines website](http://jsonlines.org/examples/):
```json
{"name": "Gilbert", "wins": [["straight", "7♣"], ["one pair", "10♥"]]}
{"name": "Alexa", "wins": [["two pair", "4♠"], ["two pair", "9♠"]]}
{"name": "May", "wins": []}
{"name": "Deloise", "wins": [["three of a kind", "5♣"]]}
```

Suppose we want to count the number of wins for each user. With normal jq, we can just use the program `jq '{name, size: .wins | length}'` to get the transformation we want.  

However, suppose we have a gigantic dataset, and this is going to take too long.  Instead, let us use hjq with the same jq program and blast a map-only job across our cluster:
```bash
hjq --mapper '{name, size: .wins | length}' --input /PATH/TO/INPUT --output /PATH/TO/OUTPUT 
```

If you use a reducer, your mapper must spit out objects with "key" and "value" fields.  Your reducer will see a sequence of arrays of `{key, value}' objects that each have the same key.  For example, suppose we wanted to count the number of times each hand type ("straight", "one pair", etc.) won a game:

```bash
hjq --mapper '{key: .wins | .[] | .[0], value: 1}' --reducer '{hand: .[0].key, count: map(.value) | add }' --input /PATH/TO/INPUT --output /PATH/TO/OUTPUT `
```

Building
========

hjq relies on [jjq](https://github.com/bskaggs/jjq) a java wrapper around the jq native library.  You can build everything with one shot using `make`.  This will produce a jar file containing everything you need; nothing needs to be installed on the individual nodes of your cluster.
