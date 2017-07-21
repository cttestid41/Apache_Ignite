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

package org.apache.ignite.internal.processors.trace;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.plugin.extensions.communication.MessageReader;
import org.apache.ignite.plugin.extensions.communication.MessageWriter;

/**
 *
 */
public class NodeTrace implements Message, IgniteTraceAware {
    /** */
    private byte[] traceIds;

    /** */
    private long[] traceStamps;

    /** */
    private int numTracePoints;

    /** */
    private List<UUID> rmtNodes;

    /** */
    private List<NodeTrace> rmtTraces;

    /**
     *
     */
    public NodeTrace() {
        traceIds = new byte[4];
        traceStamps = new long[4];
    }

    /**
     * @param point Trace point.
     */
    @Override public synchronized void recordTracePoint(TracePoint point) {
        if (traceIds == null || numTracePoints == traceIds.length)
            expandTracePoints();

        traceIds[numTracePoints] = (byte)point.ordinal();
        traceStamps[numTracePoints] = System.nanoTime() / 1000;

        numTracePoints++;
    }

    /**
     * @param rmtNode Remote node to collect.
     * @param rmtTrace Remote trace to collect.
     */
    public synchronized void addRemoteTrace(UUID rmtNode, NodeTrace rmtTrace) {
        if (rmtNodes == null) {
            assert rmtTraces == null;

            rmtNodes = new ArrayList<>(3);
            rmtTraces = new ArrayList<>(3);
        }

        rmtNodes.add(rmtNode);
        rmtTraces.add(rmtTrace);
    }

    /** {@inheritDoc} */
    @Override public short directType() {
        return -62;
    }

    /** {@inheritDoc} */
    @Override public void onAckReceived() {
        // No-op.
    }

    /**
     *
     */
    private void expandTracePoints() {
        if (traceIds == null) {
            assert traceStamps == null;

            traceIds = new byte[4];
            traceStamps = new long[4];
        }
        else {
            byte[] cp = new byte[traceIds.length * 2];

            System.arraycopy(traceIds, 0, cp, 0, traceIds.length);

            traceIds = cp;

            long[] stampCp = new long[traceStamps.length * 2];

            System.arraycopy(traceStamps, 0, stampCp, 0, traceStamps.length);

            traceStamps = stampCp;
        }
    }

    /** {@inheritDoc} */
    @Override public byte fieldsCount() {
        return 0;
    }

    /** {@inheritDoc} */
    @Override public boolean writeTo(ByteBuffer buf, MessageWriter writer) {
        return false;
    }

    /** {@inheritDoc} */
    @Override public boolean readFrom(ByteBuffer buf, MessageReader reader) {
        return false;
    }
}
