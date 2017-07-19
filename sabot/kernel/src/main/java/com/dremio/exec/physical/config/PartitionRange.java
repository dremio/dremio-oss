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
package com.dremio.exec.physical.config;

import com.dremio.common.expression.LogicalExpression;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class PartitionRange {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(PartitionRange.class);

  private LogicalExpression start;
  private LogicalExpression finish;

  @JsonCreator
  public PartitionRange(@JsonProperty("start") LogicalExpression start, @JsonProperty("finish") LogicalExpression finish) {
    super();
    this.start = start;
    this.finish = finish;
  }

  public LogicalExpression getStart() {
    return start;
  }

  public LogicalExpression getFinish() {
    return finish;
  }


}
