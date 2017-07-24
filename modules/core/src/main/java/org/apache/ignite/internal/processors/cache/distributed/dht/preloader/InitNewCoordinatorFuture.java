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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.cluster.ClusterTopologyCheckedException;
import org.apache.ignite.internal.managers.communication.GridIoPolicy;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCacheSharedContext;
import org.apache.ignite.internal.util.future.GridCompoundFuture;
import org.apache.ignite.internal.util.future.GridFutureAdapter;

import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;

/**
 *
 */
public class InitNewCoordinatorFuture extends GridCompoundFuture {
    /** */
    private GridDhtPartitionsFullMessage fullMsg;

    /** */
    private Set<UUID> awaited = new HashSet<>();

    /** */
    private Map<ClusterNode, GridDhtPartitionsSingleMessage> msgs = new HashMap<>();

    /** */
    private Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchMsgs;

    /** */
    private GridFutureAdapter restoreStateFut;

    /** */
    private IgniteLogger log;

    /** */
    private AffinityTopologyVersion initTopVer;

    /** */
    private Map<UUID, GridDhtPartitionExchangeId> extraNodes;

    /** */
    // TODO IGNITE-5578 backward compatibility
    private boolean restoreState = true;

    public boolean restoreState() {
        return restoreState;
    }

    /**
     * @param exchFut Current future.
     * @throws IgniteCheckedException If failed.
     */
    public void init(GridDhtPartitionsExchangeFuture exchFut) throws IgniteCheckedException {
        initTopVer = exchFut.initialVersion();

        GridCacheSharedContext cctx = exchFut.sharedContext();

        log = cctx.logger(getClass());

        boolean newAff = exchFut.localJoinExchange();

        IgniteInternalFuture<?> fut = cctx.affinity().initCoordinatorCaches(exchFut, newAff);

        if (fut != null)
            add(fut);

        DiscoCache curDiscoCache = cctx.discovery().discoCache();

        DiscoCache discoCache = exchFut.discoCache();

        List<ClusterNode> nodes = new ArrayList<>();

        synchronized (this) {
            for (ClusterNode node : discoCache.allNodes()) {
                if (!node.isLocal() && cctx.discovery().alive(node)) {
                    awaited.add(node.id());

                    nodes.add(node);
                }
            }

            if (!curDiscoCache.version().equals(discoCache.version())) {
                for (ClusterNode node : curDiscoCache.allNodes()) {
                    if (discoCache.node(node.id()) == null) {
                        awaited.add(node.id());

                        nodes.add(node);

                        if (extraNodes == null)
                            extraNodes = new HashMap<>();

                        GridDhtPartitionExchangeId exchId = new GridDhtPartitionExchangeId(node.id(),
                            EVT_NODE_JOINED,
                            new AffinityTopologyVersion(node.order()));

                        extraNodes.put(node.id(), exchId);
                    }
                }
            }

            if (!awaited.isEmpty()) {
                restoreStateFut = new GridFutureAdapter();

                add(restoreStateFut);
            }
        }

        if (!nodes.isEmpty()) {
            // TODO IGNITE-5578: merged nodes.
            GridDhtPartitionsSingleRequest req = GridDhtPartitionsSingleRequest.restoreStateRequest(exchFut.exchangeId(),
                exchFut.exchangeId());

            for (ClusterNode node : nodes) {
                try {
                    GridDhtPartitionsSingleRequest sndReq = req;

                    if (extraNodes != null && extraNodes.containsKey(node.id())) {
                        sndReq = GridDhtPartitionsSingleRequest.restoreStateRequest(
                            extraNodes.get(node.id()),
                            exchFut.exchangeId());
                    }

                    cctx.io().send(node, sndReq, GridIoPolicy.SYSTEM_POOL);
                }
                catch (ClusterTopologyCheckedException e) {
                    if (log.isDebugEnabled())
                        log.debug("Failed to send partitions request, node failed: " + node);

                    onNodeLeft(node.id());
                }
            }
        }

        markInitialized();
    }

    /**
     * @return Received messages.
     */
    Map<ClusterNode, GridDhtPartitionsSingleMessage> messages() {
        return msgs;
    }

    Map<UUID, GridDhtPartitionsSingleMessage> mergedJoinExchangeMessages() {
        return mergedJoinExchMsgs;
    }

    /**
     * @return Full message is some of nodes received it from previous coordinator.
     */
    GridDhtPartitionsFullMessage fullMessage() {
        return fullMsg;
    }

    /**
     * @param node Node.
     * @param msg Message.
     */
    public void onMessage(ClusterNode node, GridDhtPartitionsSingleMessage msg) {
        log.info("Init new coordinator, received response [node=" + node.id() + ']');

        assert msg.restoreState() : msg;

        boolean done = false;

        synchronized (this) {
            if (awaited.remove(node.id())) {
                GridDhtPartitionsFullMessage fullMsg0 = msg.finishMessage();

                if (fullMsg0 != null) {
                    assert fullMsg == null || fullMsg.resultTopologyVersion().equals(fullMsg0.resultTopologyVersion());

                    fullMsg  = fullMsg0;
                }
                else
                    msgs.put(node, msg);

                done = awaited.isEmpty();
            }

            if (done)
                onAllReceived();
        }

        if (done)
            restoreStateFut.onDone();
    }

    private void onAllReceived() {
        AffinityTopologyVersion resVer = fullMsg != null ? fullMsg.resultTopologyVersion() : initTopVer;

        for (Iterator<Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage>> it = msgs.entrySet().iterator(); it.hasNext();) {
            Map.Entry<ClusterNode, GridDhtPartitionsSingleMessage> e = it.next();

            GridDhtPartitionsSingleMessage msg = e.getValue();

            GridDhtPartitionExchangeId msgVer = extraNodes != null ? extraNodes.get(e.getKey().id()) : null;

            if (msgVer != null) {
                if (msgVer.topologyVersion().compareTo(resVer) < 0) {
                    it.remove();

                    continue;
                }

                assert msgVer.topologyVersion().compareTo(initTopVer) > 0 : msgVer;

                if (mergedJoinExchMsgs == null)
                    mergedJoinExchMsgs = new HashMap<>();

                msg.exchangeId(msgVer);

                mergedJoinExchMsgs.put(e.getKey().id(), msg);
            }
        }
    }

    /**
     * @param nodeId Failed node ID.
     */
    public void onNodeLeft(UUID nodeId) {
        log.info("Init new coordinator, node left [node=" + nodeId + ']');

        boolean done;

        synchronized (this) {
            done = awaited.remove(nodeId) && awaited.isEmpty();
        }

        if (done)
            restoreStateFut.onDone();
    }
}
