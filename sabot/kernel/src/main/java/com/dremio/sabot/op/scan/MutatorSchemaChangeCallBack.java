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
package com.dremio.sabot.op.scan;

import org.apache.arrow.vector.util.CallBack;

/**
 * Schema change callback class used from output mutator
 */
public class MutatorSchemaChangeCallBack implements CallBack {
  private boolean schemaChanged = false;

  public MutatorSchemaChangeCallBack() {

  }

  @Override
  public void doWork() {
    this.schemaChanged = true;
  }

  public boolean getSchemaChangedAndReset() {
    boolean current = this.schemaChanged;
    this.schemaChanged = false;
    return current;
  }

  public boolean getSchemaChanged() {
    return this.schemaChanged;
  }
}
