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

package org.apache.ignite.internal.processors.cache.distributed;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.IgniteNodeAttributes;
import org.apache.ignite.internal.TestRecordingCommunicationSpi;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.processors.cache.GridCachePartitionExchangeManager;
import org.apache.ignite.internal.processors.cache.distributed.dht.preloader.GridDhtPartitionsFullMessage;
import org.apache.ignite.internal.util.typedef.F;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
import org.apache.ignite.lang.IgniteBiPredicate;
import org.apache.ignite.plugin.extensions.communication.Message;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.TcpDiscoveryIpFinder;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;
import org.apache.ignite.testframework.GridTestUtils;
import org.apache.ignite.testframework.junits.common.GridCommonAbstractTest;

import static org.apache.ignite.cache.CacheAtomicityMode.ATOMIC;
import static org.apache.ignite.cache.CacheAtomicityMode.TRANSACTIONAL;
import static org.apache.ignite.cache.CacheWriteSynchronizationMode.FULL_SYNC;

/**
 *
 */
public class CacheExchangeMergeTest extends GridCommonAbstractTest {
    /** */
    private static TcpDiscoveryIpFinder ipFinder = new TcpDiscoveryVmIpFinder(true);

    /** */
    private ThreadLocal<Boolean> client = new ThreadLocal<>();

    /** */
    private boolean testSpi;

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

        if (testSpi)
            cfg.setCommunicationSpi(new TestRecordingCommunicationSpi());

        Boolean clientMode = client.get();

        if (clientMode != null) {
            cfg.setClientMode(clientMode);

            client.set(null);
        }

        cfg.setCacheConfiguration(
            cacheConfiguration("c1", ATOMIC, 0),
            cacheConfiguration("c2", ATOMIC, 1),
            cacheConfiguration("c3", ATOMIC, 2),
            cacheConfiguration("c4", ATOMIC, 10),
            cacheConfiguration("c5", TRANSACTIONAL, 0),
            cacheConfiguration("c6", TRANSACTIONAL, 1),
            cacheConfiguration("c7", TRANSACTIONAL, 2),
            cacheConfiguration("c8", TRANSACTIONAL, 10));

        return cfg;
    }

    /** {@inheritDoc} */
    @Override protected void afterTest() throws Exception {
        stopAllGrids();

        super.afterTest();
    }

    /**
     * @param name Cache name.
     * @param atomicityMode Cache atomicity mode.
     * @param backups Number of backups.
     * @return Cache configuration.
     */
    private CacheConfiguration cacheConfiguration(String name, CacheAtomicityMode atomicityMode, int backups) {
        CacheConfiguration ccfg = new CacheConfiguration(name);

        ccfg.setAtomicityMode(atomicityMode);
        ccfg.setWriteSynchronizationMode(FULL_SYNC);
        ccfg.setBackups(backups);

        return ccfg;
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServerAndClientJoin1() throws Exception {
        final IgniteEx srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, 3);

        IgniteInternalFuture<?> fut1 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(1);

                return null;
            }
        }, 1, "start-srv");

        waitForExchangeStart(srv0, 2);

        IgniteInternalFuture<?> fut2 = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                client.set(true);

                startGrid(2);

                return null;
            }
        }, 1, "start-srv");

        fut1.get();
        fut2.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersJoin1() throws Exception {
        IgniteEx srv0 = startGrid(0);

        mergeExchangeWaitVersion(srv0, 3);

        final AtomicInteger idx = new AtomicInteger(1);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(idx.getAndIncrement());

                return null;
            }
        }, 2, "start-node");

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServerJoin1ClientsInTopology() throws Exception {
        IgniteEx srv0 = startGrid(0);

        client.set(true);

        startGrid(1);

        client.set(true);

        startGrid(2);

        mergeExchangeWaitVersion(srv0, 5);

        final AtomicInteger idx = new AtomicInteger(3);

        IgniteInternalFuture<?> fut = GridTestUtils.runMultiThreadedAsync(new Callable<Void>() {
            @Override public Void call() throws Exception {
                startGrid(idx.getAndIncrement());

                return null;
            }
        }, 2, "start-node");

        fut.get();

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersFail1_1() throws Exception {
        mergeServersFail1(false);
    }

    /**
     * @throws Exception If failed.
     */
    public void testMergeServersFail1_2() throws Exception {
        mergeServersFail1(true);
    }

    /**
     * @param waitRebalance Wait for rebalance end before start tested topology change.
     * @throws Exception If failed.
     */
    private void mergeServersFail1(boolean waitRebalance) throws Exception {
        final Ignite srv0 = startGrids(4);

        if (waitRebalance)
            awaitPartitionMapExchange();

        mergeExchangeWaitVersion(srv0, 6);

        stopGrid(getTestIgniteInstanceName(3), true, false);
        stopGrid(getTestIgniteInstanceName(2), true, false);

        checkCaches();
    }

    /**
     * @throws Exception If failed.
     */
    public void testNoMergeJoinExchangeCoordinatorChange1() throws Exception {
        mergeJoinExchangeCoordinatorChange(4, CoordinatorChangeMode.NEW_CRD_RCDV);
    }

    /**
     * @throws Exception If failed.
     */
    private void mergeJoinExchangeCoordinatorChange(int srvs, CoordinatorChangeMode mode) throws Exception {
        log.info("mergeJoinExchangeCoordinatorChange [nodes=" + srvs + ", mode=" + mode + ']');

        testSpi = true;

        final int nodes = srvs;

        startGrids(nodes);

        CountDownLatch latch = blockExchangeFinish(srvs, mode);

        IgniteInternalFuture fut = GridTestUtils.runAsync(new Callable() {
            @Override public Object call() throws Exception {
                startGrid(nodes);

                return null;
            }
        });

        waitForExchangeStart(ignite(0), nodes + 1);

        if (latch != null && !latch.await(5, TimeUnit.SECONDS))
            fail("Failed to wait for expected messages.");

        stopGrid(0);

        fut.get();

        checkCaches();
    }

    private CountDownLatch blockExchangeFinish(int srvs, CoordinatorChangeMode mode) throws Exception {
        Ignite crd = ignite(0);

        long topVer = srvs + 1;

        switch (mode) {
            case NON_RCVD: {
                blockExchangeFinish(crd, topVer);

                break;
            }

            case NEW_CRD_RCDV: {
                List<Integer> finishNodes = F.asList(1);

                return blockExchangeFinish(crd, topVer, blockNodes(srvs, finishNodes), finishNodes);
            }

            case NON_CRD_RCVD: {
                assert srvs > 2 : srvs;

                List<Integer> finishNodes = F.asList(2);

                return blockExchangeFinish(crd, topVer, blockNodes(srvs, finishNodes), finishNodes);
            }

            default:
                fail();
        }

        return null;
    }

    /**
     * @param srvs
     * @param waitNodes
     * @return
     */
    private List<Integer> blockNodes(int srvs, List<Integer> waitNodes) {
        List<Integer> block = new ArrayList<>();

        for (int i = 0; i < srvs + 1; i++) {
            if (!waitNodes.contains(i))
                block.add(i);
        }

        return block;
    }

    /**
     *
     */
    enum CoordinatorChangeMode {
        /** */
        NON_RCVD,

        /** */
        NEW_CRD_RCDV,

        /** */
        NON_CRD_RCVD
    }

    /**
     * @param crd Exchange coordinator.
     * @param topVer Exchange topology version.
     */
    private void blockExchangeFinish(Ignite crd, long topVer) {
        final AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg0 = (GridDhtPartitionsFullMessage)msg;

                    return msg0.exchangeId() != null && msg0.exchangeId().topologyVersion().equals(topVer0);
                }

                return false;
            }
        });
    }

    /**
     * @param crd Exchange coordinator.
     * @param topVer Exchange topology version.
     */
    private CountDownLatch blockExchangeFinish(Ignite crd,
        long topVer,
        final List<Integer> blockNodes,
        final List<Integer> waitMsgNodes)
    {
        log.info("blockExchangeFinish [crd=" + crd.cluster().localNode().id() +
            ", block=" + blockNodes +
            ", wait=" + waitMsgNodes + ']');

        final AffinityTopologyVersion topVer0 = new AffinityTopologyVersion(topVer);

        final CountDownLatch latch = new CountDownLatch(waitMsgNodes.size());

        TestRecordingCommunicationSpi.spi(crd).blockMessages(new IgniteBiPredicate<ClusterNode, Message>() {
            @Override public boolean apply(ClusterNode node, Message msg) {
                if (msg instanceof GridDhtPartitionsFullMessage) {
                    GridDhtPartitionsFullMessage msg0 = (GridDhtPartitionsFullMessage)msg;

                    if (msg0.exchangeId() == null || !msg0.exchangeId().topologyVersion().equals(topVer0))
                        return false;

                    String name = node.attribute(IgniteNodeAttributes.ATTR_IGNITE_INSTANCE_NAME);

                    assert name != null : node;

                    for (Integer idx : blockNodes) {
                        if (name.equals(getTestIgniteInstanceName(idx)))
                            return true;
                    }

                    for (Integer idx : waitMsgNodes) {
                        if (name.equals(getTestIgniteInstanceName(idx))) {
                            log.info("Coordinators sends awaited message [node=" + node.id() + ']');

                            latch.countDown();
                        }
                    }
                }

                return false;
            }
        });

        return latch;
    }

    /**
     * @param node Node.
     * @param topVer Ready exchange version to wait for before trying to merge exchanges.
     */
    private void mergeExchangeWaitVersion(Ignite node, long topVer) {
        ((IgniteKernal)node).context().cache().context().exchange().mergeExchangesTestWaitVersion(
            new AffinityTopologyVersion(topVer, 0));
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches() throws Exception {
        checkCaches0();

        awaitPartitionMapExchange();

        checkCaches0();
    }

    /**
     * @throws Exception If failed.
     */
    private void checkCaches0() throws Exception {
        List<Ignite> nodes = G.allGrids();

        assertTrue(nodes.size() > 0);

        String[] cacheNames = {"c1", "c2", "c3", "c4", "c5", "c6", "c7", "c8"};

        ThreadLocalRandom rnd = ThreadLocalRandom.current();

        for (Ignite node : nodes) {
            for (String cacheName : cacheNames) {
                IgniteCache<Object, Object> cache = node.cache(cacheName);

                assertNotNull("No cache [node=" + node.name() +
                    ", order=" + node.cluster().localNode().order() +
                    ", cache=" + cacheName + ']', cache);

                for (int i = 0; i < 10; i++) {
                    Integer key = rnd.nextInt(100_000);

                    cache.put(key, i);

                    assertEquals(i, cache.get(key));
                }
            }
        }
    }

    /**
     * @param node Node.
     * @param topVer Exchange version.
     * @throws Exception If failed.
     */
    private void waitForExchangeStart(final Ignite node, final long topVer) throws Exception {
        final GridCachePartitionExchangeManager exch = ((IgniteKernal)node).context().cache().context().exchange();

        boolean wait = GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return exch.lastTopologyFuture().topologyVersion().topologyVersion() >= topVer;
            }
        }, 5000);

        assertTrue(wait);
    }
}
