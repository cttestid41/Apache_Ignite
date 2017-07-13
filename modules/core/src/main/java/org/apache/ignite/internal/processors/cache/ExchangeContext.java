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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.ignite.cache.affinity.Affinity;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.affinity.GridAffinityAssignmentCache;
import java.util.HashSet;
import java.util.Set;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ExchangeContext {
    /** */
    private Set<Integer> requestGrpsAffOnJoin;

    /** */
    private boolean fetchAffOnJoin;

    /** */
    private final boolean coalescing;

    /** */
    private AffinityTopologyVersion resTopVer;

    /** */
    private final Map<Integer, List<List<ClusterNode>>> affMap = new HashMap<>();

    /**
     * @param protocolVer Protocol version.
     */
    public ExchangeContext(int protocolVer) {
        fetchAffOnJoin = protocolVer == 1;
    }

    /**
     * @return {@code True} if on local join need fetch affinity per-group (old protocol),
     *      otherwise affinity is sent in {@link GridDhtPartitionsFullMessage}.
     */
    boolean fetchAffinityOnJoin() {
        return fetchAffOnJoin;
    }

    /**
     * @param grpId Cache group ID.
     */
    void addGroupAffinityRequestOnJoin(Integer grpId) {
        if (requestGrpsAffOnJoin == null)
            requestGrpsAffOnJoin = new HashSet<>();

        requestGrpsAffOnJoin.add(grpId);
    }

    /**
     * @return Groups to request affinity for.
     */
    @Nullable public Set<Integer> groupsAffinityRequestOnJoin() {
        return requestGrpsAffOnJoin;
    }

    public List<List<ClusterNode>> activeAffinity(GridCacheSharedContext cctx, GridAffinityAssignmentCache aff) {
        List<List<ClusterNode>> assignment = affMap.get(aff.groupId());

        if (assignment != null)
            return assignment;

        AffinityTopologyVersion affTopVer = aff.lastVersion();

        assert affTopVer.topologyVersion() > 0 : "Affinity is not initialized [grp=" + aff.cacheOrGroupName() +
            ", topVer=" + affTopVer + ", node=" + cctx.localNodeId() + ']';

        List<List<ClusterNode>> curAff = aff.assignments(affTopVer);

        assert aff.idealAssignment() != null : "Previous assignment is not available.";

        affMap.put(aff.groupId(), curAff);

        return curAff;
    }
}
