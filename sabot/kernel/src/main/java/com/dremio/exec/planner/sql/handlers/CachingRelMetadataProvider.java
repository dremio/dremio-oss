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
package com.dremio.exec.planner.sql.handlers;

import java.lang.ref.SoftReference;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.Metadata;
import org.apache.calcite.rel.metadata.MetadataDef;
import org.apache.calcite.rel.metadata.MetadataHandler;
import org.apache.calcite.rel.metadata.NullSentinel;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.metadata.UnboundMetadata;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.BuiltInMethod;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Multimap;

/**
 * Implementation of the {@link RelMetadataProvider}
 * interface that caches results from an underlying provider
 *
 * Derived from Calcite {@code org.apache.calcite.rel.metadata.CachingRelMetadataProvider}
 */
public class CachingRelMetadataProvider implements RelMetadataProvider {
  //~ Instance fields --------------------------------------------------------

  // Limiting concurrency to 2, as planner is single threaded, and only observer might
  // require concurrent access
  private final Map<CacheKey, CacheEntry> cache = new ConcurrentHashMap<>(16, 0.75f, 2);

  private final RelMetadataProvider underlyingProvider;

  private final RelOptPlanner planner;

  //~ Constructors -----------------------------------------------------------

  public CachingRelMetadataProvider(
      RelMetadataProvider underlyingProvider,
      RelOptPlanner planner) {
    this.underlyingProvider = underlyingProvider;
    this.planner = planner;
  }

  //~ Methods ----------------------------------------------------------------

  @Override
  public <M extends Metadata> UnboundMetadata<M>
  apply(Class<? extends RelNode> relClass,
      final Class<? extends M> metadataClass) {
    final UnboundMetadata<M> function =
        underlyingProvider.apply(relClass, metadataClass);
    if (function == null) {
      return null;
    }

    // TODO jvs 30-Mar-2006: Use meta-metadata to decide which metadata
    // query results can stay fresh until the next Ice Age.
    return new UnboundMetadata<M>() {
      @Override
      public M bind(RelNode rel, RelMetadataQuery mq) {
        final Metadata metadata = function.bind(rel, mq);
        return metadataClass.cast(
            Proxy.newProxyInstance(metadataClass.getClassLoader(),
                new Class[]{metadataClass},
                new CachingInvocationHandler(metadata)));
      }
    };
  }

  @Override
  public <M extends Metadata> Multimap<Method, MetadataHandler<M>>
  handlers(MetadataDef<M> def) {
    return underlyingProvider.handlers(def);
  }

  //~ Inner Classes ----------------------------------------------------------

  /** A key in the cache. It is wrapped into a soft reference so that GC
   * can collect the underlying value if needed.
   */
  private static class CacheKey extends SoftReference<List<Object>> {
    private final int hash;

    public CacheKey(List<Object> referent) {
      super(referent);
      this.hash = Preconditions.checkNotNull(referent).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
      if (!(obj instanceof CacheKey)) {
        return false;
      }

      CacheKey that = (CacheKey) obj;
      return Objects.equals(this.get(), that.get());
    }

    @Override
    public int hashCode() {
      return hash;
    }

  }

  /** An entry in the cache. Consists of the cached object and the timestamp
   * when the entry is valid. If read at a later timestamp, the entry will be
   * invalid and will be re-computed as if it did not exist. The net effect is a
   * lazy-flushing cache. */
  private static class CacheEntry extends SoftReference<Object> {
    private final long timestamp;

    public CacheEntry(Object result, long timestamp) {
      super(result);
      this.timestamp = timestamp;
    }
  }

  /** Implementation of {@link InvocationHandler} for calls to a
   * {@link CachingRelMetadataProvider}. Each request first looks in the cache;
   * if the cache entry is present and not expired, returns the cache entry,
   * otherwise computes the value and stores in the cache. */
  private class CachingInvocationHandler implements InvocationHandler {
    private final Metadata metadata;

    public CachingInvocationHandler(Metadata metadata) {
      this.metadata = Preconditions.checkNotNull(metadata);
    }

    @Override
    public Object invoke(Object proxy, Method method, Object[] args)
        throws Throwable {
      // Skip cycle check/caching for metadata.rel() and metadata.toString()
      if (BuiltInMethod.METADATA_REL.method.equals(method)
          || BuiltInMethod.OBJECT_TO_STRING.method.equals(method)) {
        return method.invoke(metadata, args);
      }

      if (metadata.rel() instanceof RelSubset) {
        // Do not cache RelSubset
        return method.invoke(metadata, args);
      }

      // Compute hash key.
      final ImmutableList.Builder<Object> builder = ImmutableList.builder();
      builder.add(method);
      // RelNode don't have identity but their digest is not stable...
      builder.add(metadata.rel());
      if (args != null) {
        for (Object arg : args) {
          if (arg instanceof RexNode) {
            // RexNode don't have any identity so use their digest instead
            builder.add(((RexNode) arg).toString());
          } else {
            // Replace null values because ImmutableList does not allow them.
            builder.add(NullSentinel.mask(arg));
          }
        }
      }
      final ImmutableList<Object> internalKey = builder.build();
      CacheKey key = new CacheKey(internalKey);

      long timestamp = planner.getRelMetadataTimestamp(metadata.rel());

      // Perform cache lookup.
      CacheEntry entry = cache.get(key);

      if (entry != null) {
        Object result = entry.get();
        if (result != null && timestamp == entry.timestamp) {
          if (result == NullSentinel.INSTANCE) {
            return null;
          }
          return result;
        }
      }

      // Cache miss or stale.
      try {
        Object result = method.invoke(metadata, args);
        entry = new CacheEntry(NullSentinel.mask(result), timestamp);
        cache.put(key, entry);
        return result;
      } catch (InvocationTargetException e) {
        Throwables.propagateIfPossible(e.getCause());
        throw e;
      }
    }
  }
}

// End CachingRelMetadataProvider.java
