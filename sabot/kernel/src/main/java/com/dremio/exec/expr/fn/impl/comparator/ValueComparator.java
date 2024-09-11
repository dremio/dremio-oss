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
package com.dremio.exec.expr.fn.impl.comparator;

import com.dremio.exec.expr.fn.impl.array.ArrayHelper;
import java.time.Duration;
import org.apache.arrow.memory.util.ByteFunctionHelpers;
import org.apache.arrow.vector.complex.impl.UnionListReader;
import org.apache.arrow.vector.complex.impl.UnionReader;
import org.apache.arrow.vector.complex.reader.FieldReader;
import org.apache.arrow.vector.holders.NullableDecimalHolder;
import org.apache.arrow.vector.holders.NullableVarBinaryHolder;
import org.apache.arrow.vector.holders.NullableVarCharHolder;
import org.apache.arrow.vector.types.Types.MinorType;

public class ValueComparator {
  private static final String COMPARISON_ERROR_MESSAGE = "Unable to compare %s with %s";
  private static final String COMPARISON_LIST_ERROR_MESSAGE =
      "Unable to compare list of type: %s with type: %s";

  public static int compareReaderValue(FieldReader left, FieldReader right) {
    if (!left.isSet()) {
      if (!right.isSet()) {
        return 0;
      } else {
        return 1;
      }
    } else if (!right.isSet()) {
      return -1;
    }
    // special case for union readers because it could contains different types
    if (left instanceof UnionReader || right instanceof UnionReader) {
      // if the types are different, we can't compare them directly
      if (!left.getMinorType().equals(right.getMinorType())) {
        return left instanceof UnionReader ? 1 : -1;
      }
    }
    isComparableType(left, right, COMPARISON_ERROR_MESSAGE);
    switch (left.getMinorType()) {
      case BIT:
        return new BitComparator().compare(left, right);
      case DATEMILLI:
      case TIMESTAMPSEC:
      case TIMESTAMPMILLI:
      case TIMESTAMPMICRO:
      case TIMESTAMPNANO:
        return new TimeStampComparator().compare(left, right);
      case INTERVALDAY:
      case DURATION:
        return new DurationComparator().compare(left, right);
      case TINYINT:
        return new ByteComparator().compare(left, right);
      case SMALLINT:
        return new ShortComparator().compare(left, right);
      case INT:
        return new IntComparator().compare(left, right);
      case BIGINT:
        return new LongComparator().compare(left, right);
      case FLOAT4:
        return new Float4Comparator().compare(left, right);
      case FLOAT8:
        return new Float8Comparator().compare(left, right);
      case VARBINARY:
      case LARGEVARBINARY:
      case FIXEDSIZEBINARY:
        return new VarBinaryComparator().compare(left, right);
      case VARCHAR:
      case LARGEVARCHAR:
        return new VarCharComparator().compare(left, right);
      case UINT1:
        return new UInt1Comparator().compare(left, right);
      case UINT2:
        return new UInt2Comparator().compare(left, right);
      case TIMEMILLI:
      case TIMESEC:
      case DATEDAY:
      case UINT4:
        return new IntValueComparator().compare(left, right);
      case TIMENANO:
      case TIMEMICRO:
        return new LongValueComparator().compare(left, right);
      case UINT8:
        return new UInt8Comparator().compare(left, right);
      case DECIMAL:
      case DECIMAL256:
        return new DecimalComparator().compare(left, right);
      case LIST:
        return new ListComparator().compare(left, right);
      default:
        throw new IllegalArgumentException("No default comparator for " + left.getMinorType());
    }
  }

  private static void isComparableType(FieldReader left, FieldReader right, String errorMessage) {
    MinorType leftType = left.getMinorType();
    MinorType rightType = right.getMinorType();
    if ((leftType == MinorType.INT && rightType == MinorType.BIGINT)
        || (leftType == MinorType.BIGINT && rightType == MinorType.INT)) {
      return;
    }
    if (left.getMinorType() != right.getMinorType()) {
      throw new IllegalArgumentException(
          String.format(
              errorMessage, left.getMinorType().toString(), right.getMinorType().toString()));
    }
  }

  /** Default comparator for byte. The comparison is based on values, with null comes first. */
  public static class ByteComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      if (!left.isSet()) {
        if (!right.isSet()) {
          return 0;
        } else {
          return 1;
        }
      } else if (!right.isSet()) {
        return -1;
      }
      return Byte.compare(left.readByte(), right.readByte());
    }
  }

  /**
   * Default comparator for short integers. The comparison is based on values, with null comes
   * first.
   */
  public static class ShortComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Short.compare(left.readShort(), right.readShort());
    }
  }

  /**
   * Default comparator for 32-bit integers. The comparison is based on int values, with null comes
   * first.
   */
  public static class IntComparator extends FieldReaderComparator {
    @Override
    public int compare(FieldReader left, FieldReader right) {
      if (right.getMinorType() == MinorType.BIGINT) {
        return Long.compare(left.readInteger(), right.readLong());
      } else {
        return Integer.compare(left.readInteger(), right.readInteger());
      }
    }
  }

  /**
   * Default comparator for long integers. The comparison is based on values, with null comes first.
   */
  public static class LongComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      if (right.getMinorType() == MinorType.INT) {
        return Long.compare(left.readLong(), right.readInteger());
      } else {
        return Long.compare(left.readLong(), right.readLong());
      }
    }
  }

  /**
   * Default comparator for unsigned bytes. The comparison is based on values, with null comes
   * first.
   */
  public static class UInt1Comparator extends FieldReaderComparator {
    @Override
    public int compare(FieldReader left, FieldReader right) {
      return (left.readByte() & 0xff) - (right.readByte() & 0xff);
    }
  }

  /**
   * Default comparator for unsigned short integer. The comparison is based on values, with null
   * comes first.
   */
  public static class UInt2Comparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return (left.readCharacter() & 0xffff) - (right.readCharacter() & 0xffff);
    }
  }

  /**
   * Default comparator for unsigned long integer. The comparison is based on values, with null
   * comes first.
   */
  public static class UInt8Comparator extends FieldReaderComparator {
    @Override
    public int compare(FieldReader left, FieldReader right) {
      return ByteFunctionHelpers.unsignedLongCompare(left.readLong(), right.readLong());
    }
  }

  /**
   * Default comparator for float type. The comparison is based on values, with null comes first.
   */
  public static class Float4Comparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Float.compare(left.readFloat(), right.readFloat());
    }
  }

  /**
   * Default comparator for double type. The comparison is based on values, with null comes first.
   */
  public static class Float8Comparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Double.compare(left.readDouble(), right.readDouble());
    }
  }

  /**
   * Default comparator for Decimal type. The comparison is based on values, with null comes first.
   */
  public static class DecimalComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      NullableDecimalHolder leftDecimal = new NullableDecimalHolder();
      NullableDecimalHolder rightDecimal = new NullableDecimalHolder();
      left.read(leftDecimal);
      right.read(rightDecimal);
      return com.dremio.exec.util.DecimalUtils.compareSignedDecimalInLittleEndianBytes(
          leftDecimal.buffer,
          org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(leftDecimal.start),
          rightDecimal.buffer,
          org.apache.arrow.memory.util.LargeMemoryUtil.capAtMaxInt(rightDecimal.start));
    }
  }

  /** Default comparator for bit type. The comparison is based on values, with null comes first. */
  public static class BitComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Boolean.compare(left.readBoolean(), right.readBoolean());
    }
  }

  /**
   * Default comparator for Duration type. The comparison is based on values, with null comes first.
   */
  public static class DurationComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      Duration value1 = left.readDuration();
      Duration value2 = right.readDuration();

      return value1.compareTo(value2);
    }
  }

  public static class IntValueComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Integer.compare(left.readInteger(), right.readInteger());
    }
  }

  public static class LongValueComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return Long.compare(left.readLong(), right.readLong());
    }
  }

  /**
   * Default comparator for TimeSec type. The comparison is based on values, with null comes first.
   */
  public static class TimeStampComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      return left.readLocalDateTime().compareTo(right.readLocalDateTime());
    }
  }

  /**
   * Default comparator for VarChar type. The comparison is based on values, with null comes first.
   */
  public static class VarCharComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      NullableVarCharHolder leftVarChar = new NullableVarCharHolder();
      NullableVarCharHolder rightVarChar = new NullableVarCharHolder();
      left.read(leftVarChar);
      right.read(rightVarChar);
      return org.apache.arrow.memory.util.ByteFunctionHelpers.compare(
          leftVarChar.buffer,
          leftVarChar.start,
          leftVarChar.end,
          rightVarChar.buffer,
          rightVarChar.start,
          rightVarChar.end);
    }
  }

  public static class VarBinaryComparator extends FieldReaderComparator {

    @Override
    public int compare(FieldReader left, FieldReader right) {
      NullableVarBinaryHolder leftVarBinary = new NullableVarBinaryHolder();
      NullableVarBinaryHolder rightVarBinary = new NullableVarBinaryHolder();
      left.read(leftVarBinary);
      right.read(rightVarBinary);
      return org.apache.arrow.memory.util.ByteFunctionHelpers.compare(
          leftVarBinary.buffer,
          leftVarBinary.start,
          leftVarBinary.end,
          rightVarBinary.buffer,
          rightVarBinary.start,
          rightVarBinary.end);
    }
  }

  /**
   * Default comparator for list vector. The comparison is based on values, with null comes first.
   */
  public static class ListComparator extends FieldReaderComparator {
    @Override
    public int compare(FieldReader left, FieldReader right) {
      if (!left.isSet()) {
        if (!right.isSet()) {
          return 0;
        } else {
          return 1;
        }
      } else if (!right.isSet()) {
        return -1;
      }
      UnionListReader leftList = (UnionListReader) left;
      UnionListReader rightList = (UnionListReader) right;
      int length1 = left.size();
      int length2 = right.size();
      int startIdx1 = leftList.getPosition();
      int startIdx2 = rightList.getPosition();
      int firstIdx1 = ArrayHelper.getFirstListPosition(leftList);
      int firstIdx2 = ArrayHelper.getFirstListPosition(rightList);
      isComparableType(leftList.reader(), rightList.reader(), COMPARISON_LIST_ERROR_MESSAGE);

      try {
        int length = length1 < length2 ? length1 : length2;
        for (int i = 0; i < length; i++) {
          leftList.reader().setPosition(firstIdx1 + i);
          rightList.reader().setPosition(firstIdx2 + i);
          int result = ValueComparator.compareReaderValue(leftList.reader(), rightList.reader());
          if (result != 0) {
            return result;
          }
        }
        return length1 - length2;
      } finally {
        leftList.setPosition(startIdx1);
        rightList.setPosition(startIdx2);
      }
    }
  }
}
