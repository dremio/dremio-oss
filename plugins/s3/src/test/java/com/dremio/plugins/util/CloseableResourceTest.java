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
package com.dremio.plugins.util;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import org.junit.Test;

import com.dremio.common.AutoCloseables;

/**
 * Tests for {@link CloseableResource}
 */
public class CloseableResourceTest {

    @Test
    public void testSingleRef() {
        final AtomicBoolean closedFlag = new AtomicBoolean(false);
        final CloseableResource<Object> res = new CloseableResource<>(new Object(), o -> closedFlag.set(true));
        AutoCloseables.close(RuntimeException.class, res);
        assertTrue(closedFlag.get());
    }

    @Test
    public void testMultiRef() {
        final AtomicBoolean closedFlag = new AtomicBoolean(false);
        final CloseableResource<Object> res = new CloseableResource<>(new Object(), o -> closedFlag.set(true));
        IntStream.range(0, 10).forEach(i -> res.incrementRef());
        IntStream.range(0, 10).forEach(i -> AutoCloseables.close(RuntimeException.class, res));
        assertFalse(closedFlag.get());

        AutoCloseables.close(RuntimeException.class, res);
        assertTrue(closedFlag.get());
    }
}
