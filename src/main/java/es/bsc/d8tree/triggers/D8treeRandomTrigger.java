package es.bsc.d8tree.triggers;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.serializers.Int32Serializer;
import org.apache.cassandra.serializers.IntegerSerializer;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Simple - stateless - trigger for the D8tree.
 */
public class D8treeRandomTrigger implements ITrigger {
    private static final int TREE_HEIGHT = Integer.getInteger("loop_tree.height", 10);
    private static final int NODE_SIZE=Integer.getInteger("loop_tree.row-limit",1000);
    private final static AtomicInteger inserted=new AtomicInteger(0);
    private static int number_of_node=-1;

    private static final Collection<Mutation> emptyReturn=ImmutableList.of();
    private final static Logger log = LoggerFactory.getLogger(D8treeRandomTrigger.class);

    public Collection<Mutation> augment(Partition partition) {
        if(number_of_node<0){
            number_of_node= Gossiper.instance.getLiveMembers().size();
        }
        log.debug("Partition is of {} type", partition.getClass().getSimpleName());
        final String cf = partition.metadata().cfName;
        if (partition.isEmpty()) {
            return emptyReturn;
        }
      /*  if (!cf.matches(".*_d8tree$")) {
            log.warn("D8treeSimpleTrigger called on the wrong table: {}", cf);
            return emptyReturn;
        }*/
        try {
            String key = ByteBufferUtil.string(partition.partitionKey().getKey());
            if (key.length() < TREE_HEIGHT) {
                log.debug("Not leaf insertions, let's ignore it.");
                return emptyReturn;
            } else {
                ArrayList<Mutation> mutations = new ArrayList<>();
                if (partition instanceof PartitionUpdate) {
                    PartitionUpdate pu = (PartitionUpdate) partition;
                    /**
                     * This is the new for creating the higher cube "_"
                     */
                    ColumnDefinition r=null;
                    for(ColumnDefinition cd :  pu.columns()){
                     if(cd.isClusteringColumn()&&cd.cfName.equals("rand")){
                         r=cd;
                         break;
                     }
                    }
                    final ColumnDefinition rand=r;

                    ClusteringComparator comparator = pu.metadata().getKeyValidatorAsClusteringComparator();
                    DecoratedKey dkey = pu.metadata().partitioner.decorateKey(CFMetaData.serializePartitionKey(comparator.make("_")));
                    PartitionUpdate rowsNew = new PartitionUpdate(pu.metadata(), dkey, pu.columns(), pu.operationCount());
                    mutations.add(new Mutation(rowsNew));
                    final int stored=inserted.addAndGet(rowsNew.dataSize())*number_of_node;
                    for (int i = 1; i < key.length(); i++) {
                        DecoratedKey dk = pu.metadata().partitioner.decorateKey(CFMetaData.serializePartitionKey(comparator.make(key.substring(0, i))));
                        PartitionUpdate rows = new PartitionUpdate(pu.metadata(), dk, pu.columns(), pu.operationCount());
                        // The percentage of data that can be stored at maximum in this level.
                        double levelPC=((double) (1<<(3*i))*NODE_SIZE)/stored;
                        int limit=((int) ((levelPC - 0.5) * 0xfffffffeL));
                        if(levelPC>1.0){
                            pu.iterator().forEachRemaining(rows::add);
                        }else{
                            pu.iterator().forEachRemaining(row->{
                                if(Int32Serializer.instance.deserialize(row.getCell(rand).value())>limit) {
                                    rows.add(row);
                                }
                            });

                        }

                        mutations.add(new Mutation(rows));
                    }
                } else {
                    log.warn("Partition it is not an PartitionUpdate");
                }
                log.debug("Returning {} mutations", mutations.size());
                return Collections.unmodifiableList(mutations);
            }

        } catch (CharacterCodingException e) {
            log.warn("Wrong partition key size");
            return emptyReturn;
        }
    }
}
