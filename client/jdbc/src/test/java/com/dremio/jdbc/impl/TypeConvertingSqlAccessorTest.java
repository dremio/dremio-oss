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
package com.dremio.jdbc.impl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

import com.dremio.common.types.TypeProtos.MajorType;
import com.dremio.common.types.TypeProtos.MinorType;
import com.dremio.common.types.Types;
import com.dremio.exec.vector.accessor.AbstractSqlAccessor;
import com.dremio.exec.vector.accessor.InvalidAccessException;
import com.dremio.exec.vector.accessor.SqlAccessor;
import com.dremio.jdbc.SQLConversionOverflowException;

/**
 * Class-level unit test for {@link TypeConvertingSqlAccessor}.
 * (Also see {@link com.dremio.jdbc.ResultSetGetMethodConversionsTest}.
 */
public class TypeConvertingSqlAccessorTest {

  /**
   * Base test stub(?) for accessors underlying TypeConvertingSqlAccessor.
   * Carries type and (Object form of) one value.
   */
  private abstract static class BaseStubAccessor extends AbstractSqlAccessor implements SqlAccessor {
    private final MajorType type;
    private final Object value;

    BaseStubAccessor( MajorType type, Object value )
    {
      this.type = type;
      this.value = value;
    }

    @Override
    public Class<?> getObjectClass() {
      throw new RuntimeException( "Unexpected use of getObjectClass(...)" );
    }

    @Override
    public MajorType getType() {
      return type;
    }

    protected Object getValue() {
      return value;
    }

    @Override
    public boolean isNull( int rowOffset ) {
      return false;
    }

    @Override
    public Object getObject( int rowOffset ) throws InvalidAccessException {
      throw new RuntimeException( "Unexpected use of getObject(...)" );
    }

  }

  // Byte?  TinyInt?  TINYINT?
  private static class TinyIntStubAccessor extends BaseStubAccessor {
    TinyIntStubAccessor( byte value ) {
      super( Types.optional(MinorType.TINYINT), value );
    }

    @Override
    public byte getByte( int rowOffset ) {
      return (Byte) getValue();
    }
  } // TinyIntStubAccessor


  // Short?  SmallInt?  SMALLINT?
  private static class SmallIntStubAccessor extends BaseStubAccessor {
    SmallIntStubAccessor( short value ) {
      super(Types.optional(MinorType.SMALLINT), value);
    }

    @Override
    public short getShort( int rowOffset ) {
      return (Short) getValue();
    }
  } // SmallIntStubAccessor


  // Int?  Int?  INT?
  private static class IntegerStubAccessor extends BaseStubAccessor {
    IntegerStubAccessor( int value ) {
      super(Types.optional(MinorType.INT), value);
    }

    @Override
    public int getInt( int rowOffset ) {
      return (Integer) getValue();
    }
  } // IntegerStubAccessor


  // Long?  Bigint?  BIGINT?
  private static class BigIntStubAccessor extends BaseStubAccessor {
    BigIntStubAccessor( long value ) {
//      super( new MajorType( MinorType.BIGINT , DataMode.REQUIRED), value );
      super(Types.optional(MinorType.BIGINT), value);
    }

    @Override
    public long getLong( int rowOffset ) {
      return (Long) getValue();
    }
  } // BigIntStubAccessor


  // Float?  Float4?  FLOAT? (REAL?)
  private static class FloatStubAccessor extends BaseStubAccessor {
    FloatStubAccessor( float value ) {
      super( Types.optional(MinorType.FLOAT4), value);
    }

    @Override
    public float getFloat( int rowOffset ) {
      return (Float) getValue();
    }
  } // FloatStubAccessor

  // Double?  Float8?  DOUBLE?
  private static class DoubleStubAccessor extends BaseStubAccessor {
    DoubleStubAccessor( double value ) {
      super(Types.optional(MinorType.FLOAT8), value);
    }

    @Override
    public double getDouble( int rowOffset ) {
      return (double) getValue();
    }
  } // DoubleStubAccessor


  //////////////////////////////////////////////////////////////////////
  // Column accessor (getXxx(...)) methods, in same order as in JDBC 4.2 spec.
  // TABLE B-6 ("Use of ResultSet getter Methods to Retrieve JDBC Data Types"):

  ////////////////////////////////////////
  // - getByte:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - ROWID;

  @Test
  public void test_getByte_on_TINYINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) 127));
    assertThat(uut1.getByte(0)).isEqualTo((byte) 127);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) -128));
    assertThat(uut2.getByte(0)).isEqualTo((byte) -128);
  }

  @Test
  public void test_getByte_on_SMALLINT_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) 127));
    assertThat(uut.getByte(0)).isEqualTo((byte) 127);
  }

  @Test
  public void test_getByte_on_SMALLINT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) 128));
    assertThatThrownBy(() -> uut.getByte(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("128")
      .hasMessageContaining("getByte")
      .hasMessageContaining("short")
      .hasMessageContaining("SMALLINT");
  }

  @Test
  public void test_getByte_on_INTEGER_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-128));
    assertThat(uut.getByte(0)).isEqualTo((byte) -128);
  }

  @Test
  public void test_getByte_on_INTEGER_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-129));
    assertThatThrownBy(() -> uut.getByte(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-129")
      .hasMessageContaining("getByte")
      .hasMessageContaining("int")
      .hasMessageContaining("INTEGER");
  }

  @Test
  public void test_getByte_on_BIGINT_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(-128));
    assertThat(uut.getByte(0)).isEqualTo((byte) -128);
  }

  @Test
  public void test_getByte_on_BIGINT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(129));
    assertThatThrownBy(() -> uut.getByte(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("129")
      .hasMessageContaining("getByte")
      .hasMessageContaining("long")
      .hasMessageContaining("BIGINT");
  }

  @Test
  public void test_getByte_on_FLOAT_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(-128.0f));
    assertThat(uut.getByte(0)).isEqualTo((byte) -128);
  }

  @Test
  public void test_getByte_on_FLOAT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(-130f));
    assertThatThrownBy(() -> uut.getByte(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-130")
      .hasMessageContaining("getByte")
      .hasMessageContaining("float")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("REAL"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }

  @Test
  public void test_getByte_on_DOUBLE_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(127.0d));
    assertThat(uut.getByte(0)).isEqualTo((byte) 127);
  }

  @Test
  public void test_getByte_on_DOUBLE_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
        new TypeConvertingSqlAccessor( new DoubleStubAccessor( -130 ) );
    assertThatThrownBy(() -> uut.getByte(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-130")
      .hasMessageContaining("getByte")
      .hasMessageContaining("double")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("DOUBLE PRECISION"),
        e -> assertThat(e.getMessage()).contains("FLOAT("));
  }

  ////////////////////////////////////////
  // - getShort:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  @Test
  public void test_getShort_on_TINYINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) 127));
    assertThat(uut1.getShort(0)).isEqualTo((short) 127);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) -128));
    assertThat(uut2.getShort(0)).isEqualTo((short) -128);
  }

  @Test
  public void test_getShort_on_SMALLINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) 32767));
    assertThat(uut1.getShort(0)).isEqualTo((short) 32767);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) -32768));
    assertThat(uut2.getShort(0)).isEqualTo((short) -32768);
  }

  @Test
  public void test_getShort_on_INTEGER_thatFits_getsIt()
    throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(32767));
    assertThat(uut1.getShort(0)).isEqualTo((short) 32767);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-32768));
    assertThat(uut2.getShort(0)).isEqualTo((short) -32768);
  }

  @Test
  public void test_getShort_on_INTEGER_thatOverflows_throws() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-32769));
    assertThatThrownBy(() -> uut.getShort(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-32769")
      .hasMessageContaining("getShort")
      .hasMessageContaining("int")
      .hasMessageContaining("INTEGER");
  }

  @Test
  public void test_getShort_BIGINT_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(-32678));
    assertThat(uut.getShort(0)).isEqualTo((short) -32678);
  }

  @Test
  public void test_getShort_on_BIGINT_thatOverflows_throws() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(65535));
    assertThatThrownBy(() -> uut.getShort(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("65535")
      .hasMessageContaining("getShort")
      .hasMessageContaining("long")
      .hasMessageContaining("BIGINT");
  }

  @Test
  public void test_getShort_on_FLOAT_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(-32768f));
    assertThat(uut.getShort(0)).isEqualTo((short) -32768);
  }

  @Test
  public void test_getShort_on_FLOAT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(-32769f));
    assertThatThrownBy(() -> uut.getShort(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-32769")
      .hasMessageContaining("getShort")
      .hasMessageContaining("float")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("REAL"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }

  @Test
  public void test_getShort_on_DOUBLE_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(32767d));
    assertThat(uut.getShort(0)).isEqualTo((short) 32767);
  }

  @Test
  public void test_getShort_on_DOUBLE_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(32768));
    assertThatThrownBy(() -> uut.getShort(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("32768")
      .hasMessageContaining("getShort")
      .hasMessageContaining("double")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("DOUBLE PRECISION"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }


  ////////////////////////////////////////
  // - getInt:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  @Test
  public void test_getInt_on_TINYINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) 127));
    assertThat(uut1.getInt(0)).isEqualTo(127);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) -128));
    assertThat(uut2.getInt(0)).isEqualTo(-128);
  }

  @Test
  public void test_getInt_on_SMALLINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) 32767));
    assertThat(uut1.getInt(0)).isEqualTo(32767);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) -32768));
    assertThat(uut2.getInt(0)).isEqualTo(-32768);
  }

  @Test
  public void test_getInt_on_INTEGER_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(2147483647));
    assertThat(uut1.getInt(0)).isEqualTo(2147483647);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-2147483648));
    assertThat(uut2.getInt(0)).isEqualTo(-2147483648);
  }

  @Test
  public void test_getInt_on_BIGINT_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(2147483647));
    assertThat(uut.getInt(0)).isEqualTo(2147483647);
  }

  @Test
  public void test_getInt_on_BIGINT_thatOverflows_throws() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(2147483648L));
    assertThatThrownBy(() -> uut.getInt(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("2147483648")
      .hasMessageContaining("getInt")
      .hasMessageContaining("long")
      .hasMessageContaining("BIGINT");
  }

  @Test
  public void test_getInt_on_FLOAT_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(1e9f));
    assertThat(uut.getInt(0)).isEqualTo(1_000_000_000);
  }

  @Test
  public void test_getInt_on_FLOAT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(1e10f));
    assertThatThrownBy(() -> uut.getInt(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("1.0E10")
      .hasMessageContaining("getInt")
      .hasMessageContaining("float")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("REAL"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }

  @Test
  public void test_getInt_on_DOUBLE_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(-2147483648.0d));
    assertThat(uut.getInt(0)).isEqualTo(-2147483648);
  }

  @Test
  public void test_getInt_on_DOUBLE_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(-2147483649.0d));
    assertThatThrownBy(() -> uut.getInt(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("-2.147483649E9")
      .hasMessageContaining("getInt")
      .hasMessageContaining("double")
      .hasMessageContaining("DOUBLE PRECISION");
  }


  ////////////////////////////////////////
  // - getLong:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  @Test
  public void test_getLong_on_TINYINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) 127));
    assertThat(uut1.getLong(0)).isEqualTo(127L);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new TinyIntStubAccessor((byte) -128));
    assertThat(uut2.getLong(0)).isEqualTo(-128L);
  }

  @Test
  public void test_getLong_on_SMALLINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) 32767));
    assertThat(uut1.getLong(0)).isEqualTo(32767L);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new SmallIntStubAccessor((short) -32768));
    assertThat(uut2.getLong(0)).isEqualTo(-32768L);
  }

  @Test
  public void test_getLong_on_INTEGER_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(2147483647));
    assertThat(uut1.getLong(0)).isEqualTo(2147483647L);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new IntegerStubAccessor(-2147483648));
    assertThat(uut2.getLong(0)).isEqualTo(-2147483648L);
  }

  @Test
  public void test_getLong_on_BIGINT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new BigIntStubAccessor(2147483648L));
    assertThat(uut.getLong(0)).isEqualTo(2147483648L);
  }

  @Test
  public void test_getLong_on_FLOAT_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(
        new FloatStubAccessor(9223372036854775807L * 1.0f));
    assertThat(uut.getLong(0)).isEqualTo(9223372036854775807L);
  }

  @Test
  public void test_getLong_on_FLOAT_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(1.5e20f));
    assertThatThrownBy(() -> uut.getLong(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("1.5000")
      .hasMessageContaining("getLong")
      .hasMessageContaining("float")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("REAL"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }

  @Test
  public void test_getLong_on_DOUBLE_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(
        new DoubleStubAccessor(9223372036854775807L * 1.0d));
    assertThat(uut.getLong(0)).isEqualTo(9223372036854775807L);
  }

  @Test
  public void test_getLong_on_DOUBLE_thatOverflows_rejectsIt() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(1e20));
    assertThatThrownBy(() -> uut.getLong(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("1.0E20")
      .hasMessageContaining("getLong")
      .hasMessageContaining("double")
      .hasMessageContaining("DOUBLE PRECISION");
  }



  ////////////////////////////////////////
  // - getFloat:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  @Test
  public void test_getFloat_on_FLOAT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(1.23f));
    assertThat(uut1.getFloat(0)).isEqualTo(1.23f);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(Float.MAX_VALUE));
    assertThat(uut2.getFloat(0)).isEqualTo(Float.MAX_VALUE);
    final SqlAccessor uut3 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(Float.MIN_VALUE));
    assertThat(uut3.getFloat(0)).isEqualTo(Float.MIN_VALUE);
  }

  @Test
  public void test_getFloat_on_DOUBLE_thatFits_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(1.125));
    assertThat(uut1.getFloat(0)).isEqualTo(1.125f);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(Float.MAX_VALUE));
    assertThat(uut2.getFloat(0)).isEqualTo(Float.MAX_VALUE);
  }

  @Test
  public void test_getFloat_on_DOUBLE_thatOverflows_throws() {
    final SqlAccessor uut =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(1e100));
    assertThatThrownBy(() -> uut.getFloat(0))
      .isInstanceOf(SQLConversionOverflowException.class)
      .hasMessageContaining("1.0E100")
      .hasMessageContaining("getFloat")
      .hasMessageContaining("double")
      .satisfiesAnyOf(e -> assertThat(e.getMessage()).contains("DOUBLE PRECISION"),
        e -> assertThat(e.getMessage()).contains("FLOAT"));
  }


  ////////////////////////////////////////
  // - getDouble:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  @Test
  public void test_getDouble_on_FLOAT_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(6.02e23f));
    assertThat(uut1.getDouble(0)).isEqualTo((double) 6.02e23f);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(Float.MAX_VALUE));
    assertThat(uut2.getDouble(0)).isEqualTo((double) Float.MAX_VALUE);
    final SqlAccessor uut3 =
      new TypeConvertingSqlAccessor(new FloatStubAccessor(Float.MIN_VALUE));
    assertThat(uut3.getDouble(0)).isEqualTo((double) Float.MIN_VALUE);
  }

  @Test
  public void test_getDouble_on_DOUBLE_getsIt() throws InvalidAccessException {
    final SqlAccessor uut1 =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(-1e100));
    assertThat(uut1.getDouble(0)).isEqualTo(-1e100);
    final SqlAccessor uut2 =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(Double.MAX_VALUE));
    assertThat(uut2.getDouble(0)).isEqualTo(Double.MAX_VALUE);
    final SqlAccessor uut3 =
      new TypeConvertingSqlAccessor(new DoubleStubAccessor(Double.MIN_VALUE));
    assertThat(uut3.getDouble(0)).isEqualTo(Double.MIN_VALUE);
  }

  ////////////////////////////////////////
  // - getBigDecimal:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  ////////////////////////////////////////
  // - getBoolean:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;

  ////////////////////////////////////////
  // - getString:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - DATE, TIME, TIMESTAMP;
  //   - DATALINK;

  ////////////////////////////////////////
  // - getNString:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - DATE, TIME, TIMESTAMP;
  //   - DATALINK;

  ////////////////////////////////////////
  // - getBytes:
  //   - BINARY, VARBINARY, LONGVARBINARY;

  ////////////////////////////////////////
  // - getDate:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - DATE, TIMESTAMP;

  ////////////////////////////////////////
  // - getTime:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - TIME, TIMESTAMP;

  ////////////////////////////////////////
  // - getTimestamp:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - DATE, TIME, TIMESTAMP;

  ////////////////////////////////////////
  // - getAsciiStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;

  ////////////////////////////////////////
  // - getBinaryStream:
  //   - BINARY, VARBINARY, LONGVARBINARY;

  ////////////////////////////////////////
  // - getCharacterStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - SQLXML;

  ////////////////////////////////////////
  // - getNCharacterStream:
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - SQLXML;

  ////////////////////////////////////////
  // - getClob:
  //   - CLOB, NCLOB;

  ////////////////////////////////////////
  // - getNClob:
  //   - CLOB, NCLOB;

  ////////////////////////////////////////
  // - getBlob:
  //   - BLOB;

  ////////////////////////////////////////
  // - getArray:
  //   - ARRAY;

  ////////////////////////////////////////
  // - getRef:
  //   - REF;

  ////////////////////////////////////////
  // - getURL:
  //   - DATALINK;

  ////////////////////////////////////////
  // - getObject:
  //   - TINYINT, SMALLINT, INTEGER, BIGINT; REAL, FLOAT, DOUBLE; DECIMAL, NUMERIC;
  //   - BIT, BOOLEAN;
  //   - CHAR, VARCHAR, LONGVARCHAR;
  //   - NCHAR, NVARCHAR, LONGNVARCHAR;
  //   - BINARY, VARBINARY, LONGVARBINARY;
  //   - CLOB, NCLOB;
  //   - BLOB;
  //   - DATE, TIME, TIMESTAMP;
  //   - TIME_WITH_TIMEZONE;
  //   - TIMESTAMP_WITH_TIMEZONE;
  //   - DATALINK;
  //   - ROWID;
  //   - SQLXML;
  //   - ARRAY;
  //   - REF;
  //   - STRUCT;
  //   - JAVA_OBJECT;

  ////////////////////////////////////////
  // - getRowId:
  //   - ROWID;

  ////////////////////////////////////////
  // - getSQLXML:
  //   - SQLXML SQLXML

}
