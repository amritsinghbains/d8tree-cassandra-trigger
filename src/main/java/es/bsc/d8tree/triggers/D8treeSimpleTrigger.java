package es.bsc.d8tree.triggers;

import com.google.common.collect.ImmutableList;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.db.ClusteringComparator;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.Partition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.triggers.ITrigger;
import org.apache.cassandra.utils.ByteBufferUtil;
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
    private static final int TREE_HEIGHT = Integer.getInteger("loop_tree.height", 10);

    private static final Collection<Mutation> emptyReturn=ImmutableList.of();
    private final static Logger log = LoggerFactory.getLogger(D8treeSimpleTrigger.class);

    public Collection<Mutation> augment(Partition partition) {
        log.debug("Partition is of {} type", partition.getClass().getSimpleName());
        final String cf = partition.metadata().cfName;
        if (partition.isEmpty()) {
            return emptyReturn;
        }
        if (!cf.matches(".*_d8tree$")) {
            log.warn("D8treeSimpleTrigger called on the wrong table: {}", cf);
            return emptyReturn;
        }
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
                    ClusteringComparator comparator = pu.metadata().getKeyValidatorAsClusteringComparator();
                    DecoratedKey dkey = pu.metadata().partitioner.decorateKey(CFMetaData.serializePartitionKey(comparator.make("_")));
                    PartitionUpdate rows_ = new PartitionUpdate(pu.metadata(), dkey, pu.columns(), pu.operationCount());
                    pu.iterator().forEachRemaining(rows_::add);
                    mutations.add(new Mutation(rows_));
                    for (int i = 1; i < key.length(); i++) {
                        DecoratedKey dk = pu.metadata().partitioner.decorateKey(CFMetaData.serializePartitionKey(comparator.make(key.substring(0, i))));
                        PartitionUpdate rows = new PartitionUpdate(pu.metadata(), dk, pu.columns(), pu.operationCount());
                        pu.iterator().forEachRemaining(rows::add);
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
