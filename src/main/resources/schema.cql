CREATE TABLE data_d8tree ( key text, rand int, value text, PRIMARY KEY(key,rand) );
CREATE TRIGGER datad8tree ON prova.data_d8tree USING 'es.bsc.d8tree.triggers.D8treeSimpleTrigger';