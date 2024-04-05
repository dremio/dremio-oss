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
package com.dremio.service.namespace.catalogstatusevents.events;

import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEvent;
import com.dremio.service.namespace.catalogstatusevents.CatalogStatusEventTopic;
import com.dremio.service.namespace.source.proto.SourceConfig;

public class SourceDeletionCatalogStatusEvent implements CatalogStatusEvent {

  private static final String TOPIC_NAME = "SOURCE_DELETION";

  private static final CatalogStatusEventTopic EVENT_TOPIC =
      new CatalogStatusEventTopic(TOPIC_NAME);

  private final SourceConfig sourceConfig;

  SourceDeletionCatalogStatusEvent(SourceConfig sourceConfig) {
    this.sourceConfig = sourceConfig;
  }

  public static SourceDeletionCatalogStatusEvent of(SourceConfig sourceConfig) {
    return new SourceDeletionCatalogStatusEvent(sourceConfig);
  }

  public SourceConfig getSourceConfig() {
    return sourceConfig;
  }

  @Override
  public CatalogStatusEventTopic getTopic() {
    return EVENT_TOPIC;
  }

  public static CatalogStatusEventTopic getEventTopic() {
    return EVENT_TOPIC;
  }
}
