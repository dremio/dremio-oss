/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
package com.dremio.sabot.op.sender;

import com.dremio.exec.physical.base.Sender;
import com.dremio.exec.record.BatchSchema;
import com.dremio.sabot.op.spi.TerminalOperator;
import com.google.common.base.Preconditions;

public abstract class BaseSender implements TerminalOperator {
  private final Sender popConfig;

  protected BaseSender(Sender popConfig) {
    this.popConfig = popConfig;
  }

  protected void checkSchema(BatchSchema schema) {
    Preconditions.checkState(schema.equals(popConfig.getSchema()),
      String.format("Schema does not match expected schema:\nExpected:%s\nActual:%s",
        popConfig.getSchema().toStringVerbose(),
        schema.toStringVerbose()));
  }
}
