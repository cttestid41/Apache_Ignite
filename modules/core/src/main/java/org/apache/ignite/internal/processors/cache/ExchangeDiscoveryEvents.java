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

package org.apache.ignite.internal.processors.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.events.DiscoveryEvent;
import org.apache.ignite.internal.managers.discovery.DiscoCache;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsExchangeFuture;
import org.apache.ignite.internal.util.typedef.internal.CU;

import static org.apache.ignite.events.EventType.EVT_NODE_FAILED;
import static org.apache.ignite.events.EventType.EVT_NODE_JOINED;
import static org.apache.ignite.events.EventType.EVT_NODE_LEFT;
import static org.apache.ignite.internal.events.DiscoveryCustomEvent.EVT_DISCOVERY_CUSTOM_EVT;

/**
 *
 */
public class ExchangeDiscoveryEvents {
    /** */
    private AffinityTopologyVersion topVer;

    /** */
    private AffinityTopologyVersion srvEvtTopVer;

    /** */
    private DiscoCache discoCache;

    /** */
    private DiscoveryEvent lastEvt;

    /** */
    private DiscoveryEvent lastSrvEvt;

    /** */
    private List<DiscoveryEvent> evts = new ArrayList<>();

    /** */
    private boolean srvJoin;

    /** */
    private boolean srvLeft;

    /**
     * @param fut Future.
     */
    ExchangeDiscoveryEvents(GridDhtPartitionsExchangeFuture fut) {
        addEvent(fut.topologyVersion(), fut.discoveryEvent(), fut.discoCache());
    }

    boolean groupAddedOnExchange(int grpId, UUID rcvdFrom) {
        for (DiscoveryEvent evt : evts) {
            if (evt.type() == EVT_NODE_JOINED && rcvdFrom.equals(evt.eventNode().id()))
                return true;
        }

        return false;
    }

    public AffinityTopologyVersion waitRebalanceEventVersion() {
        return srvEvtTopVer != null ? srvEvtTopVer : topVer;
    }

    void addEvent(AffinityTopologyVersion topVer, DiscoveryEvent evt, DiscoCache cache) {
        evts.add(evt);

        this.topVer = topVer;
        this.lastEvt = evt;
        this.discoCache = cache;

        if (evt.type() != EVT_DISCOVERY_CUSTOM_EVT) {
            ClusterNode node = evt.eventNode();

            if (!CU.clientNode(node)) {
                lastSrvEvt = evt;

                srvEvtTopVer = new AffinityTopologyVersion(evt.topologyVersion(), 0);

                if (evt.type()== EVT_NODE_JOINED)
                    srvJoin = true;
                else {
                    assert evt.type() == EVT_NODE_LEFT || evt.type() == EVT_NODE_FAILED : evt;

                    srvLeft = !CU.clientNode(node);
                }
            }
        }
    }

    public List<DiscoveryEvent> events() {
        return evts;
    }

    public DiscoCache discoveryCache() {
        return discoCache;
    }

    public DiscoveryEvent lastEvent() {
        return lastSrvEvt != null ? lastSrvEvt : lastEvt;
    }

    public AffinityTopologyVersion topologyVersion() {
        return topVer;
    }

    public boolean serverJoin() {
        return srvJoin;
    }

    public boolean serverLeft() {
        return srvLeft;
    }
}
