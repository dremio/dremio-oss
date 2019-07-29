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
package com.dremio.resource;

import java.io.Closeable;
import java.io.IOException;

/**
 * To return allocated resources
 */
public interface ResourceSet extends Closeable {
  /**
   * Returns the per-node memory limit for the query
   */
  long getPerNodeQueryMemoryLimit();

  @Override
  /**
   * At this point Resource Allocation will be considered complete
   * all unused resources released, query ends from resource allocation prospective
   */
  void close() throws IOException;

  /**
   * NoOp Implementation if needed to operations that don't deal with resource allocations
   */
  class ResourceSetNoOp implements ResourceSet {
    @Override
    public long getPerNodeQueryMemoryLimit() {
      return Long.MAX_VALUE;
    }

    @Override
    public void close() throws IOException {

    }
  }
}


