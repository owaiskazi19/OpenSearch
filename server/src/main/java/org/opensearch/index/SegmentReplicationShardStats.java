/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * SegRep stats for a single shard.
 *
 * @opensearch.internal
 */
public class SegmentReplicationShardStats implements Writeable, ToXContentFragment {
    private final String nodeId;
    private final long checkpointsBehindCount;
    private final long bytesBehindCount;
    private final long currentReplicationTimeMillis;
    private final long lastCompletedReplicationTimeMillis;

    public SegmentReplicationShardStats(
        String nodeId,
        long checkpointsBehindCount,
        long bytesBehindCount,
        long currentReplicationTimeMillis,
        long lastCompletedReplicationTime
    ) {
        this.nodeId = nodeId;
        this.checkpointsBehindCount = checkpointsBehindCount;
        this.bytesBehindCount = bytesBehindCount;
        this.currentReplicationTimeMillis = currentReplicationTimeMillis;
        this.lastCompletedReplicationTimeMillis = lastCompletedReplicationTime;
    }

    public SegmentReplicationShardStats(StreamInput in) throws IOException {
        this.nodeId = in.readString();
        this.checkpointsBehindCount = in.readVLong();
        this.bytesBehindCount = in.readVLong();
        this.currentReplicationTimeMillis = in.readVLong();
        this.lastCompletedReplicationTimeMillis = in.readVLong();
    }

    public String getNodeId() {
        return nodeId;
    }

    public long getCheckpointsBehindCount() {
        return checkpointsBehindCount;
    }

    public long getBytesBehindCount() {
        return bytesBehindCount;
    }

    public long getCurrentReplicationTimeMillis() {
        return currentReplicationTimeMillis;
    }

    public long getLastCompletedReplicationTimeMillis() {
        return lastCompletedReplicationTimeMillis;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("node_id", nodeId);
        builder.field("checkpoints_behind", checkpointsBehindCount);
        builder.field("bytes_behind", new ByteSizeValue(bytesBehindCount).toString());
        builder.field("current_replication_time", new TimeValue(currentReplicationTimeMillis));
        builder.field("last_completed_replication_time", new TimeValue(lastCompletedReplicationTimeMillis));
        builder.endObject();
        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(nodeId);
        out.writeVLong(checkpointsBehindCount);
        out.writeVLong(bytesBehindCount);
        out.writeVLong(currentReplicationTimeMillis);
        out.writeVLong(lastCompletedReplicationTimeMillis);
    }

    @Override
    public String toString() {
        return "SegmentReplicationShardStats{"
            + "nodeId='"
            + nodeId
            + '\''
            + ", checkpointsBehindCount="
            + checkpointsBehindCount
            + ", bytesBehindCount="
            + bytesBehindCount
            + ", currentReplicationLag="
            + currentReplicationTimeMillis
            + ", lastCompletedLag="
            + lastCompletedReplicationTimeMillis
            + '}';
    }
}
