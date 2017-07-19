/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.dac.service.errors;

import com.dremio.dac.model.common.ResourcePath;
import com.dremio.service.accelerator.proto.AccelerationId;

/**
 * An exception thrown when an acceleration associated with the given id is not found.
 */
public class AccelerationNotFoundException extends NotFoundException {
  public AccelerationNotFoundException(final AccelerationId id) {
    super(ResourcePath.defaultImpl(String.format("id@%s", id.getId())), "acceleration");
  }

  public AccelerationNotFoundException(final String resourceName) {
    super(ResourcePath.defaultImpl(resourceName), "acceleration");
  }
}
