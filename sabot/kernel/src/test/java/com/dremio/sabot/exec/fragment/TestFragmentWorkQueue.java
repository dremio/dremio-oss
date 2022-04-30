/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.dremio.sabot.exec.fragment;

import static com.google.common.util.concurrent.Runnables.doNothing;
import static org.junit.Assert.assertEquals;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.memory.BufferAllocator;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import com.dremio.sabot.threads.sharedres.SharedResourceGroup;
import com.dremio.sabot.threads.sharedres.SharedResourceManager;
import com.dremio.test.AllocatorRule;

/**
 * Tests for {@link FragmentWorkQueue}
 */
public class TestFragmentWorkQueue {
    private BufferAllocator testAllocator;

    @Rule
    public final AllocatorRule allocatorRule = AllocatorRule.defaultAllocator();

    @Before
    public void setupBeforeTest() {
        testAllocator = allocatorRule.newAllocator("test-fragmentworkqueue", 0, Long.MAX_VALUE);
    }

    @After
    public void cleanupAfterTest() {
        testAllocator.close();
    }
    @Test
    public void testNoAcceptanceAfterRetire() throws InterruptedException {
        ExecutorService ex = Executors.newFixedThreadPool(10);
        CountDownLatch latch = new CountDownLatch(100);
        try(ArrowBuf buf = testAllocator.buffer(64)) {
            FragmentWorkQueue workQueue = new FragmentWorkQueue(getSharedResourceGrp());
            for (int i = 0; i < 100; i++) {
                ex.execute(() -> {
                    buf.getReferenceManager().retain();
                    workQueue.put(doNothing(), buf::close);
                    latch.countDown();
                });
                if (i==50) {
                    workQueue.retire(); // current messages are cleared and subsequent messages are closed on arrival.
                }
            }
            latch.await(5, TimeUnit.MINUTES);
            ex.shutdownNow();
            // not calling retire again, but expecting any outstanding refs on the buf.
            assertEquals(1, buf.refCnt());
        }
    }

    private SharedResourceGroup getSharedResourceGrp() {
        SharedResourceManager resourceManager = SharedResourceManager.newBuilder()
                .addGroup("test")
                .build();
        return resourceManager.getGroup("test");
    }
}
