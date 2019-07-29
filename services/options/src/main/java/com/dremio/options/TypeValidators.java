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

import static com.google.common.base.Preconditions.checkArgument;

import java.util.Locale;

import com.dremio.common.exceptions.UserException;
import com.dremio.options.OptionValue.Kind;
import com.dremio.options.OptionValue.OptionType;
import com.google.common.collect.ImmutableSet;

/**
 * To help dealing with validation options of different types
 */
public class TypeValidators {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(TypeValidators.class);

  /**
   * PositiveLongValidator
   */
  public static class PositiveLongValidator extends LongValidator {
    private final long max;

    public PositiveLongValidator(String name, long max, long def) {
      super(name, def);
      checkArgument(max > 0, "max has to be a strictly positive value");
      checkArgument(def >= 0 && def <= max, "def has to be positive value less than or equal to max");
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.getNumVal() > max || v.getNumVal() < 1) {
        throw UserException.validationError()
          .message(String.format("Option %s must be between %d and %d.", getOptionName(), 1, max))
          .build(logger);
      }
    }
  }

  /**
   * PowerOfTwoLongValidator
   */
  public static class PowerOfTwoLongValidator extends PositiveLongValidator {

    public PowerOfTwoLongValidator(String name, long max, long def) {
      super(name, max, def);
      checkArgument(isPowerOfTwo(max), "max has to be a power of two number");
      checkArgument(isPowerOfTwo(def), "def has to be a power of two number");
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (!isPowerOfTwo(v.getNumVal())) {
        throw UserException.validationError()
          .message(String.format("Option %s must be a power of two.", getOptionName()))
          .build(logger);
      }
    }

    private static boolean isPowerOfTwo(long num) {
      return (num & (num - 1)) == 0;
    }
  }

  /**
   * RangeDoubleValidator
   */
  public static class RangeDoubleValidator extends DoubleValidator {
    private final double min;
    private final double max;

    public RangeDoubleValidator(String name, double min, double max, double def) {
      super(name, def);
      checkArgument(min <= max, "min has to be less than or equal to max");
      checkArgument(min <= def && def <= max, "def has to be between min and max");
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.getFloatVal() > max || v.getFloatVal() < min) {
        throw UserException.validationError()
          .message(String.format("Option %s must be between %f and %f.", getOptionName(), min, max))
          .build(logger);
      }
    }
  }

  /**
   * BooleanValidator
   */
  public static class BooleanValidator extends TypeValidator {
    public BooleanValidator(String name, boolean def) {
      super(name, Kind.BOOLEAN, OptionValue.createBoolean(OptionType.SYSTEM, name, def));
    }
  }

  /**
   * StringValidator
   */
  public static class StringValidator extends TypeValidator {
    public StringValidator(String name, String def) {
      super(name, Kind.STRING, OptionValue.createString(OptionType.SYSTEM, name, def));
    }
  }

  /**
   * LongValidator
   */
  public static class LongValidator extends TypeValidator {
    public LongValidator(String name, long def) {
      super(name, Kind.LONG, OptionValue.createLong(OptionType.SYSTEM, name, def));
    }
  }

  /**
   * DoubleValidator
   */
  public static class DoubleValidator extends TypeValidator {
    public DoubleValidator(String name, double def) {
      super(name, Kind.DOUBLE, OptionValue.createDouble(OptionType.SYSTEM, name, def));
    }
  }

  /**
   * RangeLongValidator
   */
  public static class RangeLongValidator extends LongValidator {
    private final long min;
    private final long max;

    public RangeLongValidator(String name, long min, long max, long def) {
      super(name, def);
      checkArgument(min <= max, "min has to be less than or equal to max");
      checkArgument(min <= def && def <= max, "def has to be between min and max");
      this.min = min;
      this.max = max;
    }

    @Override
    public void validate(OptionValue v) {
      super.validate(v);
      if (v.getNumVal() > max || v.getNumVal() < min) {
        throw UserException.validationError()
          .message(String.format("Option %s must be between %d and %d.", getOptionName(), min, max))
          .build(logger);
      }
    }
  }

  /**
   * AdminOptionValidator
   */
  public static class AdminOptionValidator extends StringValidator {
    public AdminOptionValidator(String name, String def) {
      super(name, def);
    }

    @Override
    public void validate(OptionValue v) {
      if (v.getType() != OptionType.SYSTEM) {
        throw UserException.validationError()
          .message("Admin related settings can only be set at SYSTEM level scope. Given scope '%s'.", v.getType())
          .build(logger);
      }
      super.validate(v);
    }
  }

  /**
   * Boolean validator which can only be set at the SYSTEM level scope
   */
  public static class AdminBooleanValidator extends BooleanValidator {
    public AdminBooleanValidator(String name, boolean def) {
      super(name, def);
    }

    @Override
    public void validate(OptionValue v) {
      if (v.getType() != OptionType.SYSTEM) {
        throw UserException.validationError()
          .message("Admin related settings can only be set at SYSTEM level scope. Given scope '%s'.", v.getType())
          .build(logger);
      }
      super.validate(v);
    }
  }

  /**
   * Wrapper {@link OptionValidator} to make sure the given option is set only of type {@link OptionType#QUERY}
   */
  public static class QueryLevelOptionValidation extends OptionValidator {
    private final OptionValidator inner;

    public QueryLevelOptionValidation(OptionValidator inner) {
      super(inner.getOptionName());
      this.inner = inner;
    }

    @Override
    public String getOptionName() {
      return inner.getOptionName();
    }

    @Override
    public boolean isShortLived() {
      return inner.isShortLived();
    }

    @Override
    public int getTtl() {
      return inner.getTtl();
    }

    @Override
    public OptionValue getDefault() {
      return inner.getDefault();
    }

    @Override
    public void validate(OptionValue v) {
      if (v.getType() != OptionType.QUERY) {
        throw UserException.validationError()
          .message("Query level options can only be set at QUERY level scope. Given scope '%s'.", v.getType())
          .build(logger);
      }
      inner.validate(v);
    }
  }

  /**
   * Validator that checks if the given value is included in a list of acceptable values. Case insensitive.
   */
  public static class EnumeratedStringValidator extends StringValidator {
    private final ImmutableSet<String> valuesSet;

    public EnumeratedStringValidator(String name, String def, String firstValue, String... otherValues) {
      super(name, def);
      ImmutableSet.Builder<String> builder = ImmutableSet.builder();
      builder.add(firstValue.toLowerCase(Locale.ROOT));
      for (String value : otherValues) {
        builder.add(value.toLowerCase(Locale.ROOT));
      }

      valuesSet = builder.build();

      checkArgument(isValid(def));
    }

    @Override
    public void validate(final OptionValue v) {
      super.validate(v);
      if (!isValid(v.getStringVal())) {
        throw UserException.validationError()
          .message(String.format("Option %s must be one of: %s.", getOptionName(), valuesSet))
          .build(logger);
      }
    }

    private boolean isValid(String val) {
      return valuesSet.contains(val.toLowerCase(Locale.ROOT));
    }
  }

  /**
   * Validator that checks if the given value is included in a list of acceptable values. Case insensitive.
   */
  public static class EnumValidator<E extends Enum<E>> extends StringValidator {
    private final ImmutableSet<String> valuesSet;

    public EnumValidator(String name, Class<E> clazz, E def) {
      super(name, def.toString().toLowerCase(Locale.ROOT));

      ImmutableSet.Builder<String> builder = ImmutableSet.builder();

      for (E value : clazz.getEnumConstants()) {
        builder.add(value.toString().toLowerCase(Locale.ROOT));
      }
      valuesSet = builder.build();
    }

    @Override
    public void validate(final OptionValue v) {
      super.validate(v);
      if (!isValid(v.getStringVal())) {
        throw UserException.validationError()
          .message(String.format("Option %s must be one of: %s.", getOptionName(), valuesSet))
          .build(logger);
      }
    }

    private boolean isValid(String val) {
      return valuesSet.contains(val.toLowerCase(Locale.ROOT));
    }
  }

  /**
   * TypeValidator
   */
  public abstract static class TypeValidator extends OptionValidator {
    private final Kind kind;
    private final OptionValue defaultValue;

    public TypeValidator(final String name, final Kind kind, final OptionValue defValue) {
      super(name);
      checkArgument(defValue.getType() == OptionType.SYSTEM, "Default value must be SYSTEM type.");
      this.kind = kind;
      this.defaultValue = defValue;
    }

    @Override
    public OptionValue getDefault() {
      return defaultValue;
    }

    @Override
    public void validate(final OptionValue v) {
      if (v.getKind() != kind) {
        throw UserException.validationError()
          .message(String.format("Option %s must be of type %s but you tried to set to %s.", getOptionName(),
            kind.name(), v.getKind().name()))
          .build(logger);
      }
    }
  }
}
