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
package com.dremio.exec.vector.accessor;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;

import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.holders.NullableVarCharHolder;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.google.common.base.Charsets;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.NettyArrowBuf;

public class VarCharAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.VARCHAR);

  private final VarCharVector ac;

  public VarCharAccessor(VarCharVector vector) {
    this.ac = vector;
  }

  @Override
  public MajorType getType() {
    return TYPE;
  }

  @Override
  public boolean isNull(int index) {
    return ac.isNull(index);
  }

  @Override
  public InputStream getStream(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    NullableVarCharHolder h = new NullableVarCharHolder();
    ac.get(index, h);
    return new ByteBufInputStream(NettyArrowBuf.unwrapBuffer(h.buffer.slice(h.start, h.end)));
  }

  @Override
  public byte[] getBytes(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return ac.get(index);
  }

  @Override
  public Class<?> getObjectClass() {
    return String.class;
  }

  @Override
  public String getObject(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return getString(index);
  }

  @Override
  public InputStreamReader getReader(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return new InputStreamReader(getStream(index), Charsets.UTF_8);
  }

  @Override
  public String getString(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return new String(getBytes(index), StandardCharsets.UTF_8);
  }

}
