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
import org.jetbrains.annotations.Nullable;

/**
 *
 */
public class ExchangeContext {
    /** */
    private final boolean coalescing;

    /** */
    private AffinityTopologyVersion resTopVer;

    /** */
    private final Map<Integer, List<List<ClusterNode>>> affMap = new HashMap<>();

    /** */
    private Set<Integer> cacheGrpsOnLocStart;
//
//    private Set<UUID> joinedNodes;
//
//    public boolean nodeJoined(UUID nodeId) {
//        return joinedNodes != null && joinedNodes.contains(nodeId);
//    }

    /**
     * @param coalescing
     */
    public ExchangeContext(AffinityTopologyVersion resTopVer, boolean coalescing) {
        this.coalescing = coalescing;
        this.resTopVer = resTopVer;
    }

    public AffinityTopologyVersion resultTopologyVersion() {
        return resTopVer;
    }

    public boolean coalescing() {
        return coalescing;
    }

    public void addCacheGroupOnLocalStart(Integer grpId) {
        if (cacheGrpsOnLocStart == null)
            cacheGrpsOnLocStart = new HashSet<>();

        cacheGrpsOnLocStart.add(grpId);
    }

    @Nullable public Set<Integer> cacheGroupsOnLocalStart() {
        return cacheGrpsOnLocStart;
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
