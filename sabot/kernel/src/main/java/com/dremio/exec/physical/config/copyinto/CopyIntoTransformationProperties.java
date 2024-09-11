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
package com.dremio.exec.physical.config.copyinto;

import com.dremio.common.expression.LogicalExpression;
import com.dremio.exec.physical.config.ExtendedProperty;
import com.dremio.exec.record.BatchSchema;
import java.util.List;

public class CopyIntoTransformationProperties implements ExtendedProperty {

  private List<LogicalExpression> logicalExpressions;
  private List<String> mappings;
  private BatchSchema schema;

  private CopyIntoTransformationProperties() {
    // for serialization purposes
  }

  public CopyIntoTransformationProperties(
      List<LogicalExpression> logicalExpressions, List<String> mappings, BatchSchema schema) {
    this.logicalExpressions = logicalExpressions;
    this.mappings = mappings;
    this.schema = schema;
  }

  public List<LogicalExpression> getLogicalExpressions() {
    return logicalExpressions;
  }

  public List<String> getMappings() {
    return mappings;
  }

  public BatchSchema getSchema() {
    return schema;
  }
}
