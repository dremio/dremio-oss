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
package com.dremio.exec.store.deltalake;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Optional;

import org.apache.calcite.util.Pair;

import com.dremio.io.FSInputStream;
import com.dremio.io.file.FileSystem;
import com.dremio.io.file.Path;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Utility to read the _last_checkpoint file present in _delta_log
 * and return the last written checkpoint version
 */
public class DeltaLastCheckPointReader {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  private DeltaLastCheckPointReader() {

  }

  public static Pair<Optional<Long>, Optional<Long>> getLastCheckPoint(FileSystem fs, Path versionFilePath) throws IOException {
    if (fs.exists(versionFilePath)) {
      try (final FSInputStream lastCheckPointFs = fs.open(versionFilePath);
           final BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(lastCheckPointFs))) {
        final LastCheckpoint checkpoint = OBJECT_MAPPER.readValue(bufferedReader.readLine(), LastCheckpoint.class);
        return new Pair(Optional.of(checkpoint.version), Optional.ofNullable(checkpoint.parts));
      }
    }

    return new Pair(Optional.empty(), Optional.empty());
  }

  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class LastCheckpoint {
    @JsonProperty
    Long version;
    @JsonProperty
    Long size;
    @JsonProperty
    Long parts;

    public LastCheckpoint() {
    }
  }
}
