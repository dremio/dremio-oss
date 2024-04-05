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
import com.google.common.base.Preconditions;
import io.netty.util.internal.PlatformDependent;
import org.apache.arrow.memory.ArrowBuf;

/** Utility class to do the proper casting for read/write to sv2. */
public final class SV2UnsignedUtil {
  public static int readAtIndex(ArrowBuf buf, int index) {
    return readAtOffset(buf, index * SelectionVector2.RECORD_SIZE);
  }

  public static int readAtOffset(ArrowBuf buf, int offset) {
    return Short.toUnsignedInt(buf.getShort(offset));
  }

  public static void writeAtIndex(ArrowBuf buf, int index, int value) {
    writeAtOffset(buf, index * SelectionVector2.RECORD_SIZE, value);
  }

  public static void writeAtOffset(ArrowBuf buf, int offset, int value) {
    Preconditions.checkArgument(value < 65536);
    buf.setShort(offset, (short) value);
  }

  public static int readAtIndexUnsafe(long bufStartAddr, int index) {
    return readAtOffsetUnsafe(bufStartAddr, index * SelectionVector2.RECORD_SIZE);
  }

  public static int readAtOffsetUnsafe(long bufStartAddr, int offset) {
    return Short.toUnsignedInt(PlatformDependent.getShort(bufStartAddr + offset));
  }

  public static void writeAtIndexUnsafe(long bufStartAddr, int index, int value) {
    writeAtOffsetUnsafe(bufStartAddr, index * SelectionVector2.RECORD_SIZE, value);
  }

  public static void writeAtOffsetUnsafe(long bufStartAddr, int offset, int value) {
    assert (value < 65536);
    PlatformDependent.putShort(bufStartAddr + offset, (short) value);
  }
}
