/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.ignite.internal.processors.cache.distributed.dht.preloader;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class CacheGroupAffinity implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    private int grpId;

    /** */
    @GridDirectCollection(GridLongList.class)
    private List<GridLongList> assign;

    /**
     *
     */
    public CacheGroupAffinity() {
        // No-op.
    }

    /**
     * @param grpId Group ID.
     * @param assign0 Assignment.
     */
    CacheGroupAffinity(int grpId, List<List<ClusterNode>> assign0) {
        this.grpId = grpId;

        assign = new ArrayList<>(assign0.size());

        for (int i = 0; i < assign0.size(); i++) {
            List<ClusterNode> nodes = assign0.get(i);

            GridLongList l = new GridLongList(nodes.size());

            for (int n = 0; n < nodes.size(); n++)
                l.add(nodes.get(n).order());

            assign.add(l);
        }
    }

    /**
     * @return Cache group ID.
     */
    int groupId() {
        return grpId;
    }

    /**
     * @return Assignments.
     */
    List<GridLongList> assignments() {
        return assign;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        writer.setBuffer(buf);

        if (!writer.isHeaderWritten()) {
            if (!writer.writeHeader(directType(), fieldsCount()))
                return false;

            writer.onHeaderWritten();
        }

        switch (writer.state()) {
            case 0:
                if (!writer.writeCollection("assign", assign, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeInt("grpId", grpId))
                    return false;

                writer.incrementState();

        }

        return true;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        reader.setBuffer(buf);

        if (!reader.beforeMessageRead())
            return false;

        switch (reader.state()) {
            case 0:
                assign = reader.readCollection("assign", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                grpId = reader.readInt("grpId");

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheGroupAffinity.class);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return 128;
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 2;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }
}
