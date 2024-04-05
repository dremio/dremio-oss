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

public class SourceUpdateCatalogStatusEvent implements CatalogStatusEvent {

  private static final String TOPIC_NAME = "SOURCE_UPDATE";
  private static final CatalogStatusEventTopic EVENT_TOPIC =
      new CatalogStatusEventTopic(TOPIC_NAME);
  private final SourceConfig before;
  private final SourceConfig after;

  SourceUpdateCatalogStatusEvent(SourceConfig before, SourceConfig after) {
    this.before = before;
    this.after = after;
  }

  public static SourceUpdateCatalogStatusEvent of(SourceConfig before, SourceConfig after) {
    return new SourceUpdateCatalogStatusEvent(before, after);
  }

  public SourceConfig getBefore() {
    return before;
  }

  public SourceConfig getAfter() {
    return after;
  }

  @Override
  public CatalogStatusEventTopic getTopic() {
    return EVENT_TOPIC;
  }

  public static CatalogStatusEventTopic getEventTopic() {
    return EVENT_TOPIC;
  }
}
