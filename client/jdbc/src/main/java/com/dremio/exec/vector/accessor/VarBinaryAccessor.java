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

import org.apache.arrow.vector.VarBinaryVector;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.common.util.DremioStringUtils;

import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.NettyArrowBuf;

public class VarBinaryAccessor extends AbstractSqlAccessor {

  private static final MajorType TYPE = Types.optional(MinorType.VARBINARY);

  private final VarBinaryVector ac;

  public VarBinaryAccessor(VarBinaryVector vector) {
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
  public Class<?> getObjectClass() {
    return byte[].class;
  }

  @Override
  public Object getObject(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    return ac.getObject(index);
  }

  @Override
  public InputStream getStream(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    NullableVarBinaryHolder h = new NullableVarBinaryHolder();
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
  public String getString(int index) {
    if (ac.isNull(index)) {
      return null;
    }
    byte[] b = ac.get(index);
    return DremioStringUtils.toBinaryString(b);
  }

}
