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
package com.dremio.options;

import com.dremio.common.exceptions.UserException;
import com.dremio.options.TypeValidators.TypeValidator;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

/** Test for {@code TypeValidators} classes */
@RunWith(Enclosed.class)
public class TestTypeValidators {
  protected static OptionValue newLongValue(long value) {
    return OptionValue.createLong(OptionValue.OptionType.SYSTEM, "test-option", value);
  }

  protected static OptionValue newDoubleValue(double value) {
    return OptionValue.createDouble(OptionValue.OptionType.SYSTEM, "test-option", value);
  }

  protected static OptionValue newStringValue(String value) {
    return OptionValue.createString(OptionValue.OptionType.SYSTEM, "test-option", value);
  }

  protected static OptionValue newBooleanValue(boolean value) {
    return OptionValue.createBoolean(OptionValue.OptionType.SYSTEM, "test-option", value);
  }

  /** Tests for {@code TypeValidators.PositiveLongValidator} */
  public static class TestPositiveLongValidator {
    protected TypeValidator newValidator(long max, long def) {
      return new TypeValidators.PositiveLongValidator("test-option", max, def);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator(20, 10);
      // check no fail...
      validator.validate(newLongValue(15));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidMax() {
      newValidator(0, 2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void negativeDefault() {
      newValidator(10, -2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void zeroDefault() {
      newValidator(10, -2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooLargeDefault() {
      newValidator(10, 20);
    }

    @Test(expected = UserException.class)
    public void negativeValue() {
      TypeValidator validator = newValidator(20, 10);
      validator.validate(newLongValue(-2));
    }

    @Test(expected = UserException.class)
    public void tooLargeValue() {
      TypeValidator validator = newValidator(20, 10);
      validator.validate(newLongValue(30));
    }
  }

  /** Tests for {@code TypeValidators.PowerOfTwoLongValidator} */
  public static class TestPowerOfTwoLongValidator extends TestPositiveLongValidator {
    @Override
    protected TypeValidator newValidator(long max, long def) {
      return new TypeValidators.PowerOfTwoLongValidator("test-option", max, def);
    }

    @Override
    @Test
    public void ok() {
      TypeValidator validator = newValidator(32, 16);
      // check no fail...
      validator.validate(newLongValue(16));
    }

    @Override
    @Test(expected = UserException.class)
    public void negativeValue() {
      TypeValidator validator = newValidator(32, 16);
      validator.validate(newLongValue(-2));
    }

    @Override
    @Test(expected = UserException.class)
    public void tooLargeValue() {
      TypeValidator validator = newValidator(32, 16);
      validator.validate(newLongValue(64));
    }

    @Test(expected = IllegalArgumentException.class)
    public void notAPowerOfTwoMax() {
      newValidator(20, 16);
    }

    @Test(expected = IllegalArgumentException.class)
    public void notAPowerOfTwoDefault() {
      newValidator(32, 17);
    }
  }

  /** Tests for {@code TypeValidators.RangeLongValidator} */
  public static class TestRangeLongValidator {
    protected TypeValidator newValidator(long min, long max, long def) {
      return new TypeValidators.RangeLongValidator("test-option", min, max, def);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator(0, 20, 10);
      // check no fail...
      validator.validate(newLongValue(15));
    }

    @Test
    public void sameMinMaxDefault() {
      TypeValidator validator = newValidator(7, 7, 7);
      // check no fail...
      validator.validate(newLongValue(7));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidMax() {
      newValidator(2, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallDefault() {
      newValidator(0, 10, -2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooLargeDefault() {
      newValidator(0, 10, 12);
    }

    @Test(expected = UserException.class)
    public void tooSmallValue() {
      TypeValidator validator = newValidator(0, 10, 5);
      validator.validate(newLongValue(-2));
    }

    @Test(expected = UserException.class)
    public void tooLargeValue() {
      TypeValidator validator = newValidator(0, 20, 10);
      validator.validate(newLongValue(30));
    }
  }

  /** Tests for {@code TypeValidators.RangeDoubleValidator} */
  public static class TestRangeDoubleValidator {
    protected TypeValidator newValidator(double min, double max, double def) {
      return new TypeValidators.RangeDoubleValidator("test-option", min, max, def);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator(0, 20, 10);
      // check no fail...
      validator.validate(newDoubleValue(15));
    }

    @Test
    public void sameMinMaxDefault() {
      TypeValidator validator = newValidator(7, 7, 7);
      // check no fail...
      validator.validate(newDoubleValue(7));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidMax() {
      newValidator(2, 0, 1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooSmallDefault() {
      newValidator(0, 10, -2);
    }

    @Test(expected = IllegalArgumentException.class)
    public void tooLargeDefault() {
      newValidator(0, 10, 12);
    }

    @Test(expected = UserException.class)
    public void tooSmallValue() {
      TypeValidator validator = newValidator(0, 10, 5);
      validator.validate(newDoubleValue(-2));
    }

    @Test(expected = UserException.class)
    public void tooLargeValue() {
      TypeValidator validator = newValidator(0, 20, 10);
      validator.validate(newDoubleValue(30));
    }
  }

  /** Tests for {@code TypeValidators.AdminOptionValidator} */
  public static class TestAdminStringValidator {
    protected TypeValidator newValidator(String def) {
      return new TypeValidators.AdminStringValidator("test-option", def);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator("foo");
      // check no fail...
      validator.validate(newStringValue("bar"));
    }

    @Test(expected = UserException.class)
    public void invalidOption() {
      TypeValidator validator = newValidator("foo");
      validator.validate(
          OptionValue.createString(OptionValue.OptionType.QUERY, "test-option", "bar"));
    }
  }

  /** Tests for {@code TypeValidators.AdminBooleanValidator} */
  public static class TestAdminBooleanValidator {
    protected TypeValidator newValidator(boolean def) {
      return new TypeValidators.AdminBooleanValidator("test-option", def);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator(true);
      // check no fail...
      validator.validate(newBooleanValue(false));
    }

    @Test(expected = UserException.class)
    public void invalidOption() {
      TypeValidator validator = newValidator(true);
      validator.validate(
          OptionValue.createBoolean(OptionValue.OptionType.QUERY, "test-option", false));
    }
  }

  /** Tests for {@code TypeValidators.QueryLevelOptionValidation} */
  public static class TestQueryLevelOptionValidation {
    protected OptionValidator newValidator(String def) {
      return new TypeValidators.QueryLevelOptionValidation(
          new TypeValidators.StringValidator("test-option", def));
    }

    @Test
    public void ok() {
      OptionValidator validator = newValidator("foo");
      // check no fail...
      validator.validate(
          OptionValue.createString(OptionValue.OptionType.QUERY, "test-option", "bar"));
    }

    @Test(expected = UserException.class)
    public void invalidOption() {
      OptionValidator validator = newValidator("foo");
      validator.validate(newStringValue("bar"));
    }
  }

  /** Tests for {@code TypeValidators.AdminLevelOptionValidation} */
  public static class TestAdminPositiveLongValidator {
    protected OptionValidator newValidator(long def) {
      return new TypeValidators.AdminPositiveLongValidator("test-option", Integer.MAX_VALUE, def);
    }

    @Test
    public void ok() {
      OptionValidator validator = newValidator(10);
      // check no fail...
      validator.validate(newLongValue(2));
    }

    @Test(expected = UserException.class)
    public void invalidOption() {
      OptionValidator validator = newValidator(20);
      validator.validate(OptionValue.createLong(OptionValue.OptionType.QUERY, "test-option", 20));
    }
  }

  /** Tests for {@code TypeValidators.EnumeratedStringValidator} */
  public static class TestEnumeratedStringValidator {
    protected TypeValidator newValidator(String def, String firstValue, String... otherValues) {
      return new TypeValidators.EnumeratedStringValidator(
          "test-option", def, firstValue, otherValues);
    }

    @Test
    public void ok() {
      TypeValidator validator = newValidator("foo", "foo", "bar", "baz");
      // check no fail...
      validator.validate(newStringValue("bar"));
    }

    @Test(expected = UserException.class)
    public void invalidValue() {
      TypeValidator validator = newValidator("foo", "foo", "bar", "baz");
      validator.validate(newStringValue("bal"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidDefault() {
      new TypeValidators.EnumeratedStringValidator("test-option", "foo", "bar");
    }
  }
}
