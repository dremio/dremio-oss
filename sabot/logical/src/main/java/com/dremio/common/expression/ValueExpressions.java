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
package com.dremio.common.expression;

import static java.util.concurrent.TimeUnit.NANOSECONDS;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.util.Collections;
import java.util.Iterator;

import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;

import com.dremio.common.expression.visitors.ExprVisitor;
import com.dremio.common.types.TypeProtos;
import com.dremio.common.util.CoreDecimalUtility;
import com.dremio.common.util.DateTimes;
import com.google.common.base.Objects;

public class ValueExpressions {

  public static LogicalExpression getBigInt(long l){
    return new LongExpression(l);
  }

  public static LogicalExpression getInt(int i){
    return new IntExpression(i);
  }

  public static LogicalExpression getFloat8(double d){
    return new DoubleExpression(d);
  }
  public static LogicalExpression getFloat4(float f){
    return new FloatExpression(f);
  }

  public static LogicalExpression getBit(boolean b){
    return new BooleanExpression(Boolean.toString(b));
  }

  public static LogicalExpression getChar(String s){
    return new QuotedString(s);
  }

  public static LogicalExpression getDate(DateString date) {
    LocalDate localDate = LocalDate.parse(date.toString(), DateTimes.CALCITE_LOCAL_DATE_FORMATTER);
    return new DateExpression(localDate.atTime(LocalTime.MIDNIGHT).toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  /**
   * Parse date given in format 'YYYY-MM-DD' to millis in UTC timezone
   */
  public static long getDate(String date) {
    LocalDate localDate = LocalDate.parse(date, DateTimes.CALCITE_LOCAL_DATE_FORMATTER);
    return localDate.atTime(LocalTime.MIDNIGHT).toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static LogicalExpression getTime(TimeString time) {
    LocalTime localTime = LocalTime.parse(time.toString(), DateTimes.CALCITE_LOCAL_TIME_FORMATTER);
    return new TimeExpression(Math.toIntExact(NANOSECONDS.toMillis(localTime.toNanoOfDay())));
  }

  /**
   * Parse time given in format 'hh:mm:ss' to millis in UTC timezone
   */
  public static int getTime(String time) {
    LocalTime localTime = LocalTime.parse(time, DateTimes.CALCITE_LOCAL_TIME_FORMATTER);
    return Math.toIntExact(NANOSECONDS.toMillis(localTime.toNanoOfDay()));
  }

  public static LogicalExpression getTimeStamp(TimestampString timestamp) {
    LocalDateTime localDateTime = LocalDateTime.parse(timestamp.toString(), DateTimes.CALCITE_LOCAL_DATETIME_FORMATTER);
    return new TimeStampExpression(localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli());
  }

  /**
   * Parse date given in format 'YYYY-MM-DD hh:mm:ss' to millis in UTC timezone
   */
  public static long getTimeStamp(String timestamp) {
    LocalDateTime localDateTime = LocalDateTime.parse(timestamp, DateTimes.CALCITE_LOCAL_DATETIME_FORMATTER);
    return localDateTime.toInstant(ZoneOffset.UTC).toEpochMilli();
  }

  public static LogicalExpression getIntervalYear(int months) {
    return new IntervalYearExpression(months);
  }

  public static LogicalExpression getIntervalDay(long intervalInMillis) {
      return new IntervalDayExpression(intervalInMillis);
  }

  public static LogicalExpression getDecimal(BigDecimal i) {
    return new DecimalExpression(i);
  }

  public static LogicalExpression getNumericExpression(String sign, String s) {
    final String numStr = (sign == null) ? s : sign+s;
    final char lastChar = s.charAt(s.length()-1);

    if(lastChar == 'l'){
      return new LongExpression(Long.parseLong(numStr.substring(0, numStr.length()-1)));
    }

    if(lastChar == 'd'){
      return new DoubleExpression(Double.parseDouble(numStr.substring(0, numStr.length()-1)));
    }

    if(lastChar == 'f') {
      return new FloatExpression(Float.parseFloat(numStr.substring(0, numStr.length()-1)));
    }

    if(lastChar == 'i') {
      return new IntExpression(Integer.parseInt(numStr.substring(0, numStr.length()-1)));
    }

    if(lastChar == 'm') {
      BigDecimal decimal = new BigDecimal(numStr.substring(0, numStr.length()-1));
      return new DecimalExpression(decimal);
    }

    try {
        int a = Integer.parseInt(numStr);
        return new IntExpression(a);
    } catch (Exception e) {

    }
    try {
      long l = Long.parseLong(numStr);
      return new LongExpression(l);
    } catch (Exception e) {

    }

    try {
      float f = Float.parseFloat(numStr);
      return new FloatExpression(f);
    } catch (Exception e) {

    }

    try {
      double d = Double.parseDouble(numStr);
      return new DoubleExpression(d);
    } catch (Exception e) {

    }

    throw new IllegalArgumentException(String.format("Unable to parse string %s as integer or floating point number.",
        numStr));

  }

  protected static abstract class ValueExpression<V> extends LogicalExpressionBase {
    public final V value;

    @SuppressWarnings("rawtypes")
    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof ValueExpression)) {
        return false;
      }
      ValueExpression castOther = (ValueExpression) other;
      return Objects.equal(value, castOther.value);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(value);
    }

    protected ValueExpression(String value) {
      this.value = parseValue(value);
    }

    protected abstract V parseValue(String s);

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }


  }

  public static class BooleanExpression extends ValueExpression<Boolean> {

    public BooleanExpression(String value) {
      super(value);
    }

    @Override
    protected Boolean parseValue(String s) {
      return Boolean.parseBoolean(s);
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.BIT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitBooleanConstant(this, value);
    }

    public boolean getBoolean() {
      return value;
    }

    @Override
    public String toString() {
      return "ValueExpression[boolean=" + value + "]";
    }
  }

  public static class FloatExpression extends LogicalExpressionBase {
    private final float f;

    public FloatExpression(float f) {
      this.f = f;
    }

    public float getFloat() {
      return f;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.FLOAT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitFloatConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof FloatExpression)) {
        return false;
      }
      FloatExpression castOther = (FloatExpression) other;
      return Objects.equal(f, castOther.f);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[float=" + f + "]";
    }
  }

  public static class IntExpression extends LogicalExpressionBase {

    private int i;

    public IntExpression(int i) {
      this.i = i;
    }

    public int getInt() {
      return i;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.INT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntConstant(this, value);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[int=" + i + "]";
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof IntExpression)) {
        return false;
      }
      IntExpression castOther = (IntExpression) other;
      return Objects.equal(i, castOther.i);
    }

    @Override
    public int hashCode() {
      return Objects.hashCode(i);
    }


  }

  public static class DecimalExpression extends LogicalExpressionBase {

    private BigDecimal decimal;
    private int decimalIntValue;
    private int scale;
    private int precision;

    public DecimalExpression(BigDecimal input) {
      this.scale = input.scale();
      this.precision = input.precision();
      this.decimal = input;
      this.decimalIntValue = CoreDecimalUtility.getDecimal9FromBigDecimal(input, scale, precision);
    }

    public BigDecimal getDecimal() {
      return decimal;
    }

    public int getIntFromDecimal() {
      return decimalIntValue;
    }

    public int getScale() {
      return scale;
    }

    public int getPrecision() {
      return precision;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.fromDecimalPrecisionScale(precision, scale);
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitDecimalConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof DecimalExpression)) {
        return false;
      }
      DecimalExpression castOther = (DecimalExpression) other;
      return Objects.equal(decimal, castOther.decimal) && Objects.equal(precision, castOther.precision) && Objects.equal(scale, castOther.scale) ;
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[decimal=" + decimal + ", " + scale + ", " + precision + "]";
    }

    public TypeProtos.MajorType getMajorType() {
      return TypeProtos.MajorType.newBuilder().setMinorType(TypeProtos.MinorType.DECIMAL).setScale
        (scale).setPrecision(precision).setMode(TypeProtos.DataMode.REQUIRED).build();
    }
  }

  public static class DoubleExpression extends LogicalExpressionBase {
    private final double d;

    public DoubleExpression(double d) {
      this.d = d;
    }

    public double getDouble() {
      return d;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.DOUBLE;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitDoubleConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof DoubleExpression)) {
        return false;
      }
      DoubleExpression castOther = (DoubleExpression) other;
      return Objects.equal(d, castOther.d);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[double=" + d + "]";
    }
  }

  public static class LongExpression extends LogicalExpressionBase {

    private final long l;

    public LongExpression(long l) {
      this.l = l;
    }

    public long getLong() {
      return l;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.BIGINT;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitLongConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof LongExpression)) {
        return false;
      }
      LongExpression castOther = (LongExpression) other;
      return Objects.equal(l, castOther.l);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[long=" + l + "]";
    }
  }


  public static class DateExpression extends LogicalExpressionBase {

    private final long dateInMillis;

    public DateExpression(long dateInMillis) {
      this.dateInMillis = dateInMillis;
    }

    public long getDate() {
      return dateInMillis;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.DATE;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitDateConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof DateExpression)) {
        return false;
      }
      DateExpression castOther = (DateExpression) other;
      return Objects.equal(dateInMillis, castOther.dateInMillis);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[date=" + dateInMillis + "]";
    }

  }


  public static class TimeExpression extends LogicalExpressionBase {

    private final int timeInMillis;

    public TimeExpression(int timeInMillis) {
      this.timeInMillis = timeInMillis;
    }

    public int getTime() {
      return timeInMillis;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.TIME;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitTimeConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof TimeExpression)) {
        return false;
      }
      TimeExpression castOther = (TimeExpression) other;
      return Objects.equal(timeInMillis, castOther.timeInMillis);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[time=" + timeInMillis + "]";
    }
  }

  public static class TimeStampExpression extends LogicalExpressionBase {

    private final long timeInMillis;

    public TimeStampExpression(long timeInMillis) {
      this.timeInMillis = timeInMillis;
    }

    public long getTimeStamp() {
      return timeInMillis;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.TIMESTAMP;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitTimeStampConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof TimeStampExpression)) {
        return false;
      }
      TimeStampExpression castOther = (TimeStampExpression) other;
      return Objects.equal(timeInMillis, castOther.timeInMillis);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[timestamp=" + timeInMillis + "]";
    }

  }

  public static class IntervalYearExpression extends LogicalExpressionBase {

    private final int months;

    public IntervalYearExpression(int months) {
      this.months = months;
    }

    public int getIntervalYear() {
      return months;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.INTERVAL_YEAR_MONTHS;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntervalYearConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof IntervalYearExpression)) {
        return false;
      }
      IntervalYearExpression castOther = (IntervalYearExpression) other;
      return Objects.equal(months, castOther.months);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }


    @Override
    public String toString() {
      return "ValueExpression[interval_year=" + months + "]";
    }

  }

  public static class IntervalDayExpression extends LogicalExpressionBase {

    private static final long MILLIS_IN_DAY = 1000 * 60 * 60 * 24;

    private final int days;
    private final int millis;

    public IntervalDayExpression(long intervalInMillis) {
      this((int) (intervalInMillis / MILLIS_IN_DAY), (int) (intervalInMillis % MILLIS_IN_DAY));
    }

      public IntervalDayExpression(int days, int millis) {
      this.days = days;
      this.millis = millis;
    }

    public int getIntervalDay() {
      return days;
    }

    public int getIntervalMillis() {
        return millis;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.INTERVAL_DAY_SECONDS;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitIntervalDayConstant(this, value);
    }

    @Override
    public boolean equals(final Object other) {
      if (!(other instanceof IntervalDayExpression)) {
        return false;
      }
      IntervalDayExpression castOther = (IntervalDayExpression) other;
      return Objects.equal(days, castOther.days) && Objects.equal(millis, castOther.millis);
    }

    @Override
    public Iterator<LogicalExpression> iterator() {
      return Collections.emptyIterator();
    }

    @Override
    public String toString() {
      return "ValueExpression[interval_days=" + days + ", " + millis + "]";
    }
  }

  public static class QuotedString extends ValueExpression<String> {

    public QuotedString(String value) {
      super(value);
    }

    public String getString() {
      return value;
    }

    @Override
    protected String parseValue(String s) {
      return s;
    }

    @Override
    public CompleteType getCompleteType() {
      return CompleteType.VARCHAR;
    }

    @Override
    public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {
      return visitor.visitQuotedStringConstant(this, value);
    }

    @Override
    public String toString() {
      return "ValueExpression[quoted_string=" + value + "]";
    }
  }

}
