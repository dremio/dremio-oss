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
package com.dremio.exec.store.dfs;

import com.dremio.connector.metadata.EntityPath;
import com.dremio.context.RequestContext;
import com.google.common.collect.ImmutableList;
import io.opentelemetry.context.Context;
import io.opentelemetry.context.ContextKey;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

public class TestMetadataIOPool {

  @Test
  public void testTaskSubmittedByPoolThreadExecutesOnSameThread() throws Exception {
    try (MetadataIOPool pool = MetadataIOPool.Factory.INSTANCE.newPool(2)) {
      String taskName1 = "bulk_get_metadata_1";
      String taskName2 = "bulk_get_metadata_2";
      EntityPath entity1 = new EntityPath(ImmutableList.of("a", "b", "c"));
      pool.execute(
              new MetadataIOPool.MetadataTask<>(
                  taskName1,
                  entity1,
                  () -> {
                    Thread outer = Thread.currentThread();
                    return pool.execute(
                            new MetadataIOPool.MetadataTask<>(
                                taskName2,
                                entity1,
                                () -> {
                                  Thread inner = Thread.currentThread();
                                  Assertions.assertThat(inner).isSameAs(outer);
                                  return new Object();
                                }))
                        .toCompletableFuture()
                        .join();
                  }))
          .toCompletableFuture()
          .join();
    }
  }

  @Test
  public void testContextPropagation() throws Exception {
    try (MetadataIOPool pool = MetadataIOPool.Factory.INSTANCE.newPool(1)) {
      RequestContext requestContext = RequestContext.current().with(() -> "k", "v");
      Context traceContext = Context.current().with(ContextKey.named("k"), "v");
      requestContext.call(
          traceContext.wrap(
              () ->
                  pool.execute(
                          new MetadataIOPool.MetadataTask<>(
                              "bulk_get_metadata",
                              new EntityPath(ImmutableList.of("a", "b", "c")),
                              () -> {
                                Assertions.assertThat(RequestContext.current())
                                    .isSameAs(requestContext);
                                Assertions.assertThat(Context.current()).isSameAs(traceContext);
                                return new Object();
                              }))
                      .toCompletableFuture()
                      .join()));
    }
  }
}
