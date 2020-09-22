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
package com.dremio.exec.vector.complex.fn;

import java.io.IOException;

import org.apache.arrow.memory.ArrowBuf;
import org.apache.arrow.vector.holders.VarBinaryHolder;
import org.apache.arrow.vector.holders.VarCharHolder;

import com.google.common.base.Charsets;

public class WorkingBuffer {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(WorkingBuffer.class);

  private ArrowBuf workBuf;

  public WorkingBuffer(ArrowBuf workBuf) {
    this.workBuf = workBuf;
  }

  private void ensure(int length) {
    workBuf = workBuf.reallocIfNeeded(length);
  }

  public void prepareVarCharHolder(String value, VarCharHolder h) throws IOException {
    byte[] b = value.getBytes(Charsets.UTF_8);
    ensure(b.length);
    workBuf.setBytes(0, b);
    h.start = 0;
    h.end = b.length;
    h.buffer = workBuf;
  }

  public int prepareBinary(byte[] b){
    ensure(b.length);
    workBuf.setBytes(0, b);
    return b.length;
  }

  public int prepareVarCharHolder(String value) throws IOException {
    byte[] b = value.getBytes(Charsets.UTF_8);
    ensure(b.length);
    workBuf.setBytes(0, b);
    return b.length;
  }

  public void prepareBinary(byte[] b, VarBinaryHolder h) throws IOException {
    ensure(b.length);
    workBuf.setBytes(0, b);
    h.start = 0;
    h.end = b.length;
    h.buffer = workBuf;
  }

  public ArrowBuf getBuf(){
    return workBuf;
  }

}
