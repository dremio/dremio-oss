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
import java.util.function.Consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Wraps AutoCloseable implementation of non-closeable objects
 */
public class CloseableResource<T> implements AutoCloseable {
    private static final Logger logger = LoggerFactory.getLogger(CloseableResource.class);
    private T resource;
    private Consumer<T> closerFunc;
    private AtomicInteger refCnt = new AtomicInteger(1);

    public CloseableResource(T resource, Consumer<T> closerFunc) {
        this.resource = resource;
        this.closerFunc = closerFunc;
    }

    public void incrementRef() {
        refCnt.incrementAndGet();
        if (logger.isDebugEnabled()) {
            logger.debug("Class {} acquired the ref for {}:{}, Ref {}", getCallingClass(),
                    resource.getClass().getSimpleName(), System.identityHashCode(resource), refCnt.get());
        }
    }

    public T getResource() {
        return resource;
    }

    @Override
    public void close() throws Exception {
        if (refCnt.decrementAndGet() == 0 && resource != null) {
            logger.debug("Closing resource {}", resource.getClass().getSimpleName());
            closerFunc.accept(resource);
            resource = null;
        }
        if (logger.isDebugEnabled()) {
            logger.debug("Class {} released the ref for {}:{}, Current ref count:{}", getCallingClass(),
                    resource.getClass().getSimpleName(), System.identityHashCode(resource), refCnt.get());
        }
    }

    private String getCallingClass() {
        final StackTraceElement traceElement = Thread.currentThread().getStackTrace()[3];
        return traceElement.getClassName() + ":" + traceElement.getMethodName() + ":" + traceElement.getLineNumber();
    }
}
