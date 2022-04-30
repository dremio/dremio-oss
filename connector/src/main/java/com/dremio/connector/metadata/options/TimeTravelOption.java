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
package com.dremio.connector.metadata.options;

import java.util.stream.Stream;

import com.dremio.connector.metadata.GetDatasetOption;
import com.dremio.connector.metadata.GetMetadataOption;
import com.dremio.connector.metadata.ListPartitionChunkOption;
import com.dremio.connector.metadata.MetadataOption;

/**
 * Option that express time travel settings.
 */
public final class TimeTravelOption implements GetDatasetOption, GetMetadataOption, ListPartitionChunkOption {

  private final TimeTravelRequest timeTravelRequest;

  private TimeTravelOption(TimeTravelRequest timeTravelRequest) {
    this.timeTravelRequest = timeTravelRequest;
  }

  public TimeTravelRequest getTimeTravelRequest() {
    return timeTravelRequest;
  }

  /**
   * Time travel specifier.
   */
  public enum TimeTravelSpecifier {
    AT,
    BEFORE
  }

  /**
   * Time travel request.
   */
  public abstract static class TimeTravelRequest {
    private final TimeTravelSpecifier timeTravelSpecifier;

    private TimeTravelRequest(TimeTravelSpecifier timeTravelSpecifier) {
      this.timeTravelSpecifier = timeTravelSpecifier;
    }

    public TimeTravelSpecifier getTimeTravelSpecifier() {
      return timeTravelSpecifier;
    }
  }

  /**
   * Timestamp based time travel request.
   */
  public static final class TimestampRequest extends TimeTravelRequest {

    private final long millis;

    private TimestampRequest(TimeTravelSpecifier timeTravelSpecifier, long millis) {
      super(timeTravelSpecifier);
      this.millis = millis;
    }

    public long getTimestampMillis() {
      return millis;
    }
  }

  /**
   * Snapshot id based time travel request.
   */
  public static final class SnapshotIdRequest extends TimeTravelRequest {

    private final String snapshotId;

    private SnapshotIdRequest(TimeTravelSpecifier timeTravelSpecifier, String snapshotId) {
      super(timeTravelSpecifier);
      this.snapshotId = snapshotId;
    }

    public String getSnapshotId() {
      return snapshotId;
    }
  }

  public static TimeTravelRequest newSnapshotIdRequest(TimeTravelSpecifier timeTravelSpecifier, String snapshotId) {
    return new SnapshotIdRequest(timeTravelSpecifier, snapshotId);
  }

  public static TimeTravelRequest newTimestampRequest(TimeTravelSpecifier timeTravelSpecifier, long millis) {
    return new TimestampRequest(timeTravelSpecifier, millis);
  }

  public static TimeTravelOption newTimeTravelOption(TimeTravelRequest timeTravelRequest) {
    return new TimeTravelOption(timeTravelRequest);
  }

  public static TimeTravelOption getTimeTravelOption(MetadataOption... options) {
    return (TimeTravelOption) Stream.of(options)
      .filter(o -> o instanceof TimeTravelOption)
      .findFirst()
      .orElse(null);
  }
}
