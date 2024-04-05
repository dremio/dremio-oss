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
package com.dremio.service.namespace.catalogstatusevents;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CatalogStatusEventsImpl implements CatalogStatusEvents {
  private static final Logger LOGGER = LoggerFactory.getLogger(CatalogStatusEventsImpl.class);
  private final ConcurrentHashMap<
          CatalogStatusEventTopic, ConcurrentHashMap<Integer, CatalogStatusSubscriber>>
      topics;

  public CatalogStatusEventsImpl() {
    this.topics = new ConcurrentHashMap<>();
  }

  @Override
  public void subscribe(
      CatalogStatusEventTopic catalogStatusEventTopic, CatalogStatusSubscriber subscriber) {
    if (!topics.containsKey(catalogStatusEventTopic)) {
      topics.put(catalogStatusEventTopic, new ConcurrentHashMap<>());
    }
    topics.get(catalogStatusEventTopic).put(subscriber.hashCode(), subscriber);
  }

  @Override
  public void publish(CatalogStatusEvent event) {
    ConcurrentHashMap<Integer, CatalogStatusSubscriber> map = topics.get(event.getTopic());
    if (map == null) {
      return;
    }

    for (Map.Entry<Integer, CatalogStatusSubscriber> subscribers : map.entrySet()) {
      CatalogStatusSubscriber subscriber = subscribers.getValue();

      try {
        subscriber.onCatalogStatusEvent(event);
      } catch (Exception exception) {
        // We need to catch any exception here in order not to disturb
        // the catalog lifecycle
        LOGGER.error(
            "Failed to call subscriber {} to publish CatalogStatusEvent event {}.",
            subscriber.getClass().getSimpleName(),
            event,
            exception);
      }
    }
  }
}
