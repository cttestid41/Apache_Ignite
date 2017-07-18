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
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridDirectCollection;
import org.apache.ignite.internal.GridDirectMap;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.GridLongList;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.apache.ignite.internal.util.typedef.internal.U;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageCollectionItemType;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class CacheGroupAffinityMessage implements Message {
    /** */
    private static final long serialVersionUID = 0L;

    /** */
    @GridDirectCollection(GridLongList.class)
    private List<GridLongList> assigns;

    /** */
    @GridDirectMap(keyType = Integer.class, valueType = GridLongList.class)
    private Map<Integer, GridLongList> assignsDiff;

    /**
     *
     */
    public CacheGroupAffinityMessage() {
        // No-op.
    }

    /**
     * @param assign0 Assignment.
     */
    private CacheGroupAffinityMessage(List<List<ClusterNode>> assign0, Map<Integer, List<Long>> assignDiff0) {
        if (assign0 != null) {
            assigns = new ArrayList<>(assign0.size());

            for (int i = 0; i < assign0.size(); i++) {
                List<ClusterNode> nodes = assign0.get(i);

                GridLongList l = new GridLongList(nodes.size());

                for (int n = 0; n < nodes.size(); n++)
                    l.add(nodes.get(n).order());

                assigns.add(l);
            }
        }

        if (assignDiff0 != null) {
            assignsDiff = U.newHashMap(assignDiff0.size());

            for (Map.Entry<Integer, List<Long>> e : assignDiff0.entrySet()) {
                List<Long> orders = e.getValue();

                GridLongList l = new GridLongList(orders.size());

                for (int n = 0; n < orders.size(); n++)
                    l.add(orders.get(n));

                assignsDiff.put(e.getKey(), l);
            }
        }
    }

    public static Map<Integer, CacheGroupAffinityMessage> createAffinityDiffMessages(Map<Integer, Map<Integer, List<Long>>> affDiff) {
        if (F.isEmpty(affDiff))
            return null;

        Map<Integer, CacheGroupAffinityMessage> map = U.newHashMap(affDiff.size());

        for (Map.Entry<Integer, Map<Integer, List<Long>>> e : affDiff.entrySet())
            map.put(e.getKey(), new CacheGroupAffinityMessage(null, e.getValue()));

        return map;
    }

    /**
     * @param cctx Context.
     * @param topVer Topology version.
     * @param affReq Cache group IDs.
     * @param cachesAff Optional already prepared affinity.
     * @return Affinity.
     */
    static Map<Integer, CacheGroupAffinityMessage> createAffinityMessages(
        GridCacheSharedContext cctx,
        AffinityTopologyVersion topVer,
        Collection<Integer> affReq,
        @Nullable Map<Integer, CacheGroupAffinityMessage> cachesAff) {
        assert !F.isEmpty(affReq) : affReq;

        if (cachesAff == null)
            cachesAff = U.newHashMap(affReq.size());

        for (Integer grpId : affReq) {
            if (!cachesAff.containsKey(grpId)) {
                List<List<ClusterNode>> assign = cctx.affinity().affinity(grpId).assignments(topVer);

                cachesAff.put(grpId, new CacheGroupAffinityMessage(assign, null));
            }
        }

        return cachesAff;
    }

    public static List<ClusterNode> toNodes(GridLongList assign, Map<Long, ClusterNode> nodesByOrder, DiscoCache discoCache) {
        List<ClusterNode> assign0 = new ArrayList<>(assign.size());

        for (int n = 0; n < assign.size(); n++) {
            long order = assign.get(n);

            ClusterNode affNode = nodesByOrder.get(order);

            if (affNode == null) {
                affNode = discoCache.serverNodeByOrder(order);

                assert affNode != null : order;

                nodesByOrder.put(order, affNode);
            }

            assign0.add(affNode);
        }

        return assign0;
    }

    /**
     * @param nodesByOrder Nodes by order cache.
     * @param discoCache Discovery data cache.
     * @return Assignments.
     */
    public List<List<ClusterNode>> createAssignments(Map<Long, ClusterNode> nodesByOrder, DiscoCache discoCache) {
        List<List<ClusterNode>> assignments0 = new ArrayList<>(assigns.size());

        for (int p = 0; p < assigns.size(); p++) {
            GridLongList assign = assigns.get(p);

            assignments0.add(toNodes(assign, nodesByOrder, discoCache));
        }

        return assignments0;
    }

    public Map<Integer, GridLongList> assignmentsDiff() {
        return assignsDiff;
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
                if (!writer.writeCollection("assigns", assigns, MessageCollectionItemType.MSG))
                    return false;

                writer.incrementState();

            case 1:
                if (!writer.writeMap("assignsDiff", assignsDiff, MessageCollectionItemType.INT, MessageCollectionItemType.MSG))
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
                assigns = reader.readCollection("assigns", MessageCollectionItemType.MSG);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

            case 1:
                assignsDiff = reader.readMap("assignsDiff", MessageCollectionItemType.INT, MessageCollectionItemType.MSG, false);

                if (!reader.isLastRead())
                    return false;

                reader.incrementState();

        }

        return reader.afterMessageRead(CacheGroupAffinityMessage.class);
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

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(CacheGroupAffinityMessage.class, this);
    }
}
