/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.Service;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Interface to deal with Query lifecycle from resourcing prospective
 */
public interface ResourceAllocator extends Service {

  /**
   * To allocate resources from ResourceScheduler
   * @param queryContext
   * @param resourceSchedulingProperties
   * @return
   * @throws ResourceAllocationException
   */
  ListenableFuture<ResourceSet> allocate(final ResourceSchedulingContext queryContext,
                                         final ResourceSchedulingProperties resourceSchedulingProperties);
}
