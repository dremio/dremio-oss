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

import com.dremio.exec.physical.config.ExtendedProperty;
import com.dremio.exec.record.BatchSchema;

public class CopyErrorsExtendedProperties implements ExtendedProperty {
  private String originalJobId;
  private BatchSchema validatedTableSchema;

  public CopyErrorsExtendedProperties() {
    // for serialization purposes
  }

  public CopyErrorsExtendedProperties(String originalJobId, BatchSchema validatedTableSchema) {
    this.originalJobId = originalJobId;
    this.validatedTableSchema = validatedTableSchema;
  }

  public String getOriginalJobId() {
    return originalJobId;
  }

  public BatchSchema getValidatedTableSchema() {
    return validatedTableSchema;
  }

  public void setOriginalJobId(String originalJobId) {
    this.originalJobId = originalJobId;
  }

  public void setValidatedTableSchema(BatchSchema validatedTableSchema) {
    this.validatedTableSchema = validatedTableSchema;
  }
}
