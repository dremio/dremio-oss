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

package com.dremio.exec.expr;

import com.sun.codemodel.JBlock;

/**
 * Uses this class to keep track # of Dremio Logical Expressions that are
 * put to JBlock.
 *
 * JBlock is final class; we could not extend JBlock directly.
 */
public class SizedJBlock {
  private final JBlock block;
  private int count; // # of Logical Expressions added to this block

  public SizedJBlock(JBlock block) {
    this.block = block;
    this.count = 1;
  }

  public JBlock getBlock() {
    return this.block;
  }

  public void incCounter() {
    this.count ++;
  }

  public int getCount() {
    return this.count;
  }

}
