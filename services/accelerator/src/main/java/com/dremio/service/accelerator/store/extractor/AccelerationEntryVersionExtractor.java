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
package com.dremio.service.accelerator.store.extractor;

import com.dremio.datastore.VersionExtractor;
import com.dremio.service.accelerator.proto.AccelerationEntry;
import com.google.common.base.Optional;

/**
 * Extracts {@link AccelerationEntry} version
 */
public class AccelerationEntryVersionExtractor implements VersionExtractor<AccelerationEntry> {
  @Override
  public Long getVersion(final AccelerationEntry value) {
    return value.getDescriptor().getVersion();
  }

  @Override
  public Long incrementVersion(final AccelerationEntry value) {
    final Long current = value.getDescriptor().getVersion();
    value.getDescriptor().setVersion(Optional.fromNullable(current).or(-1L) + 1);
    return current;
  }

  @Override
  public void setVersion(final AccelerationEntry value, final Long version) {
    value.getDescriptor().setVersion(version);
  }
}
