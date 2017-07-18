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

import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.IgniteEx;
import org.apache.ignite.internal.IgniteInternalFuture;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.affinity.AffinityTopologyVersion;
import org.apache.ignite.internal.util.typedef.G;
import org.apache.ignite.internal.util.typedef.PA;
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

    /** {@inheritDoc} */
    @Override protected IgniteConfiguration getConfiguration(String igniteInstanceName) throws Exception {
        IgniteConfiguration cfg = super.getConfiguration(igniteInstanceName);

        ((TcpDiscoverySpi)cfg.getDiscoverySpi()).setIpFinder(ipFinder);

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

        GridTestUtils.waitForCondition(new PA() {
            @Override public boolean apply() {
                return srv0.context().cache().context().exchange().lastTopologyFuture().topologyVersion().topologyVersion() == 2L;
            }
        }, 5000);

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
}
