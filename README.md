# d8tree-cassandra-trigger
An experimental Cassandra trigger for D8tree

This trigger works only with a modified version of Cassandra https://github.com/cugni/cassandra/tree/cassandra-3.4-d8tree.

You should use only on a table with a string as a key, and at least clustering column. The table name shall have the "_d8tree" postfix. 
## Usage example

```cql
CREATE TABLE data_d8tree ( key text, rand int, value text, PRIMARY KEY(key,rand) );
CREATE TRIGGER datad8tree ON prova.data_d8tree USING 'es.bsc.d8tree.triggers.D8treeSimpleTrigger';

```
