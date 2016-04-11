package es.bsc.d8tree.triggers;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.RowUpdateBuilder;
import org.apache.cassandra.db.filter.ColumnFilter;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.db.rows.UnfilteredRowIterator;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.SearchIterator;
import org.apache.cassandra.utils.btree.BTreeSearchIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;

/**
 * Simple - stateless - trigger for the D8tree.
 */
public class D8treeSimpleTrigger implements ITrigger {
    private static final int TREE_HEIGHT = Integer.getInteger("loop_tree.height",10);


    private final static Logger log= LoggerFactory.getLogger(D8treeSimpleTrigger.class);
    public Collection<Mutation> augment(Partition partition) {
        final  String cf=partition.metadata().cfName;
        if(partition.isEmpty()){
            return  null;
        }
        if(!cf.matches(".*_d8tree$")){
            log.warn("D8treeSimpleTrigger called on the wrong table: {}",cf);
            return null;
        }
        try {
            String key = ByteBufferUtil.string(partition.partitionKey().getKey());
            if(key.length()<TREE_HEIGHT){
                log.debug("Not leaf insertions, let's ignore it.");
                return null;

            }else{

                ArrayList<Mutation> mutations = new ArrayList<Mutation>();
                for(int i=1; i<key.length(); i++){
                    RowUpdateBuilder audit =
                            new RowUpdateBuilder(partition.metadata(),
                            FBUtilities.timestampMicros(),
                            key.substring(0,i));


                    UnfilteredRowIterator unfilteredRowIterator = partition.unfilteredIterator();


                    while (unfilteredRowIterator.hasNext()) {
                        Unfiltered unfiltered = unfilteredRowIterator.next();
                        if(unfiltered.isRow()) {
                            Row row=(Row)unfiltered;
                            audit.clustering((Object[]) row.clustering().getRawValues());
                            for (Cell cell :row.cells()){
                                audit.add(cell.column(), cell.value());
                            }
                        }
                    }

                    mutations.add(audit.build());

                }
                return Collections.unmodifiableList(mutations);

            }

        } catch (CharacterCodingException e) {
            log.warn("Wrong partition key size");
            return null;
        }
    }
}
