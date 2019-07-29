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
package com.dremio.dac.resource;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.ObjectMapper;


/**
 * Export profiles paremeters
 */
public class ExportProfilesParams {

  /**
   * Export output format.
   */
  public static enum ExportFormatType {
    ZIP,
    JSON
  }

  /**
   * Lists a behaviour for a case, when output file already exists
   */
  public static enum WriteFileMode {
    FAIL_IF_EXISTS,
    OVERWRITE,
    SKIP
  }

  private static ObjectMapper mapper = new ObjectMapper();
  private final String outputFilePath;
  private final WriteFileMode writeMode;
  private final Long fromDate;
  private final Long toDate;
  private final ExportFormatType outputFormatType;
  private final int chunkSize;

  @JsonCreator
  public ExportProfilesParams(
    @JsonProperty("outputFilePath") String outputFilePath,
    @JsonProperty("writeMode") WriteFileMode writeFileMode,
    @JsonProperty("fromDate") Long fromDate,
    @JsonProperty("toDate") Long toDate,
    @JsonProperty("outputFormatType") ExportFormatType outputFormatType,
    @JsonProperty("chunkSize") int chunkSize) {
    this.outputFilePath = outputFilePath;
    this.writeMode = writeFileMode;
    this.fromDate = fromDate;
    this.toDate = toDate;
    this.outputFormatType = outputFormatType;
    this.chunkSize = chunkSize;
  }

  public String getOutputFilePath() {
    return outputFilePath;
  }

  public WriteFileMode getWriteMode() {
    return writeMode;
  }

  public Long getFromDate() {
    return fromDate;
  }

  public Long getToDate() {
    return toDate;
  }

  public ExportFormatType getOutputFormatType() {
    return outputFormatType;
  }

  public int getChunkSize() {
    return chunkSize;
  }

  public String toParamString() throws Exception {
    return mapper.writeValueAsString(this);
  }

  public static ExportProfilesParams fromParamString(String serialized) throws Exception {
    return mapper.readValue(serialized, ExportProfilesParams.class);
  }
}
