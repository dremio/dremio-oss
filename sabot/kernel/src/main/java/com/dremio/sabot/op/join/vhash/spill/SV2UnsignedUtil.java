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
package com.dremio.sabot.op.join.vhash.spill;

import com.dremio.exec.record.selection.SelectionVector2;

import io.netty.util.internal.PlatformDependent;

/**
 * Utility class to do the proper casting for read/write to sv2.
 */
public final class SV2UnsignedUtil {
  public static int read(long sv2Addr, int index) {
    assert index >= 0;
    return read(sv2Addr + index * SelectionVector2.RECORD_SIZE);
  }

  public static int read(long sv2OffsetAttr) {
    return Short.toUnsignedInt(PlatformDependent.getShort(sv2OffsetAttr));
  }

  public static void write(long sv2Addr, int index, int value) {
    assert value < 65536;
    write(sv2Addr + index * SelectionVector2.RECORD_SIZE, value);
  }

  public static void write(long sv2OffsetAttr, int value) {
    PlatformDependent.putShort(sv2OffsetAttr, (short)value);
  }

}
