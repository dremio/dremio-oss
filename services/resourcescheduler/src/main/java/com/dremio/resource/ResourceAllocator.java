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

import java.util.function.Consumer;

import com.dremio.options.OptionManager;
import com.dremio.resource.common.ResourceSchedulingContext;
import com.dremio.resource.exception.ResourceAllocationException;
import com.dremio.service.Service;

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
  default ResourceSchedulingResult allocate(final ResourceSchedulingContext queryContext,
                                            final ResourceSchedulingProperties resourceSchedulingProperties) {
    return allocate(queryContext, resourceSchedulingProperties,
      ResourceSchedulingObserver.NO_OP, (x) -> {/*no-op*/});
  }

  /**
   * To allocate resources from ResourceScheduler and report to an optional consumer (used for observers)
   * @param queryContext
   * @param resourceSchedulingProperties
   * @param resourceDecisionConsumer
   * @param resourceSchedulingObserver
   * @return
   */
  ResourceSchedulingResult allocate(final ResourceSchedulingContext queryContext,
                                    final ResourceSchedulingProperties resourceSchedulingProperties,
                                    final ResourceSchedulingObserver resourceSchedulingObserver,
                                    final Consumer<ResourceSchedulingDecisionInfo> resourceDecisionConsumer);

  void cancel(final ResourceSchedulingContext queryContext);

  /**
   * Get group resource information, used for planning.
   *
   * @param optionManager
   * @return resource information.
   */
  GroupResourceInformation getGroupResourceInformation(final OptionManager optionManager,
                                                       final ResourceSchedulingProperties resourceSchedulingProperties)
    throws ResourceAllocationException;

  ResourceAllocator ResourceAllocatorNOOP = new ResourceAllocator(){
    @Override
    public void close() throws Exception {

    }

    @Override
    public void start() throws Exception {

    }

    @Override
    public ResourceSchedulingResult allocate(ResourceSchedulingContext queryContext,
                                             ResourceSchedulingProperties resourceSchedulingProperties,
                                             ResourceSchedulingObserver resourceSchedulingObserver,
                                             Consumer<ResourceSchedulingDecisionInfo> resourceDecisionConsumer) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void cancel(ResourceSchedulingContext queryContext) {
      throw new UnsupportedOperationException();
    }

    @Override
    public GroupResourceInformation getGroupResourceInformation(OptionManager optionManager,
                                                                ResourceSchedulingProperties resourceSchedulingProperties) {
      return null;
    }
  };


}
