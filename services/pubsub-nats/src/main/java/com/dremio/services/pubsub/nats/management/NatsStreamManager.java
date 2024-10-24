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
package com.dremio.services.pubsub.nats.management;

import com.dremio.services.pubsub.Topic;
import com.dremio.services.pubsub.nats.exceptions.NatsErrorCodes;
import com.dremio.services.pubsub.nats.exceptions.NatsManagerException;
import com.dremio.services.pubsub.nats.reflection.ClassToInstanceUtil;
import com.google.protobuf.Message;
import io.nats.client.Connection;
import io.nats.client.JetStreamApiException;
import io.nats.client.JetStreamManagement;
import io.nats.client.Nats;
import io.nats.client.api.RetentionPolicy;
import io.nats.client.api.StorageType;
import io.nats.client.api.StreamConfiguration;
import io.nats.client.api.StreamInfo;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NatsStreamManager implements StreamManager {
  private static final Logger logger = LoggerFactory.getLogger(NatsStreamManager.class);

  private final String natsServerUrl;
  private Connection natsConnection;
  private JetStreamManagement jetStreamManagement;

  private static final Duration DEFAULT_MAX_RETENTION =
      Duration.ofDays(7); // same as for GCP pub-sub

  public NatsStreamManager(String natsServerUrl) {
    this.natsServerUrl = natsServerUrl;
  }

  public void connect() {
    try {
      this.natsConnection = Nats.connect(natsServerUrl);
      this.jetStreamManagement = natsConnection.jetStreamManagement();
    } catch (Exception e) {
      throw new NatsManagerException("Problem when connecting to NATS", e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      natsConnection.close();
    } catch (InterruptedException e) {
      throw new NatsManagerException("Problem when closing NATS connection", e);
    }
  }

  @Override
  public <M extends Message> void upsertStream(
      StreamOptions streamOptions, List<Class<? extends Topic<M>>> topics) {

    List<String> subjectNames =
        topics.stream()
            .map(ClassToInstanceUtil::toTopicInstance)
            .map(Topic::getName)
            .collect(Collectors.toList());

    StreamConfiguration streamConfig =
        StreamConfiguration.builder()
            .name(streamOptions.streamName())
            .subjects(subjectNames)
            .storageType(StorageType.File) // File storage
            .replicas(streamOptions.getNumberOfReplicasOrDefault())
            .retentionPolicy(RetentionPolicy.Limits) // Retain messages based on limits
            .maxAge(DEFAULT_MAX_RETENTION)
            .build();

    try {
      // First, attempt to get StreamInfo to check if the stream already exists
      jetStreamManagement.getStreamInfo(streamOptions.streamName());
      // If the stream exists, update it
      updateStream(streamConfig, streamOptions.streamName());
    } catch (JetStreamApiException e) {
      if (e.getApiErrorCode() == NatsErrorCodes.JET_STREAM_NOT_FOUND.getErrorCode()) {
        try {
          // If stream doesn't exist, create a new one
          addStream(streamConfig, streamOptions.streamName());
        } catch (Exception ex) {
          throw new NatsManagerException("Unexpected error during stream upsert.", ex);
        }

      } else {
        throw new NatsManagerException("JetStream API error occurred during stream upsert.", e);
      }
    } catch (Exception e) {
      throw new NatsManagerException("Unexpected error during stream upsert.", e);
    }
  }

  private void updateStream(StreamConfiguration streamConfig, String streamName)
      throws JetStreamApiException, IOException {
    StreamInfo streamInfo = jetStreamManagement.updateStream(streamConfig);
    logger.info(
        "Stream: {} updated successfully. Current state of the stream: {}",
        streamName,
        streamInfo.getStreamState());
  }

  private void addStream(StreamConfiguration streamConfig, String streamName)
      throws JetStreamApiException, IOException {
    StreamInfo streamInfo = jetStreamManagement.addStream(streamConfig);
    logger.info(
        "Stream: {} created successfully. Current state of the stream: {}",
        streamName,
        streamInfo.getStreamState());
  }
}
