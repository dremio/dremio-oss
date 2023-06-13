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
package com.dremio.dac.explore;

import java.util.ArrayList;
import java.util.List;

import com.dremio.service.jobs.JobStatusListener;
import com.dremio.service.jobs.metadata.proto.QueryMetadata;

class AsyncMetadataJobStatusListener implements JobStatusListener {
  private List<MetaDataListener> listeners;

  interface MetaDataListener {
    void metadataCollected(QueryMetadata metadata);
  }

  AsyncMetadataJobStatusListener(MetaDataListener listener) {
    listeners = new ArrayList<>();
    listeners.add(listener);
  }

  void addMetadataListener(MetaDataListener listener) {
    listeners.add(listeners.size(), listener);
  }

  @Override
  public void metadataCollected(QueryMetadata metadata) {
    Thread t = new Thread(() -> {
      for (MetaDataListener l: listeners) {
        l.metadataCollected(metadata);
      }
    });
    t.start();
  }
}
