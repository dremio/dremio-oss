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

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class helps in maintaining multiple references for a closeable object. The actual close happens only when all
 * refs have released.
 * @param <T>
 */
public class CloseableRef<T extends AutoCloseable> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CloseableRef.class);
    private AtomicInteger refCnt;
    private T obj;

    public CloseableRef(T obj) {
        if (logger.isDebugEnabled()) {
            logger.debug("Class {} acquired a new ref for {}:{}", getCallingClass(), obj.getClass().getSimpleName(), System.identityHashCode(obj));
        }
        this.obj = obj;
        refCnt = new AtomicInteger(1);
    }

    public T acquireRef() {
        if (logger.isDebugEnabled()) {
            logger.debug("Class {} acquired the ref for {}:{}", getCallingClass(), obj.getClass().getSimpleName(), System.identityHashCode(obj));
        }
        Preconditions.checkNotNull(this.obj);
        refCnt.incrementAndGet();
        return this.obj;
    }

    public void close() throws Exception {
        if (logger.isDebugEnabled()) {
            logger.debug("Class {} released the ref for {}:{}", getCallingClass(), obj.getClass().getSimpleName(), System.identityHashCode(obj));
        }
        if (refCnt.decrementAndGet() == 0 && obj != null) {
            obj.close();
            obj = null;
        }
    }

    private String getCallingClass() {
        final StackTraceElement traceElement = Thread.currentThread().getStackTrace()[3];
        return traceElement.getClassName() + ":" + traceElement.getMethodName() + ":" + traceElement.getLineNumber();
    }
}
