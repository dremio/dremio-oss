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
package com.dremio.exec.server.options;

import static com.google.common.base.Preconditions.checkArgument;

import com.dremio.options.OptionValue;
import com.dremio.options.OptionValueProto;
import com.dremio.options.OptionValueProtoList;
import java.util.Collection;

/** Utility class for {@link OptionValue}. */
final class OptionValueProtoUtils {
  private OptionValueProtoUtils() {}

  public static OptionValueProto toOptionValueProto(OptionValue optionValue) {
    checkArgument(
        optionValue.getType() == OptionValue.OptionType.SYSTEM,
        String.format(
            "Invalid OptionType. OptionType must be 'SYSTEM', was given '%s'",
            optionValue.getType()));

    final OptionValue.Kind kind = optionValue.getKind();
    final OptionValueProto.Builder builder =
        OptionValueProto.newBuilder().setName(optionValue.getName());

    switch (kind) {
      case BOOLEAN:
        builder.setBoolVal(optionValue.getBoolVal());
        break;
      case LONG:
        builder.setNumVal(optionValue.getNumVal());
        break;
      case STRING:
        builder.setStringVal(optionValue.getStringVal());
        break;
      case DOUBLE:
        builder.setFloatVal(optionValue.getFloatVal());
        break;
      default:
        throw new IllegalArgumentException("Invalid OptionValue kind");
    }
    return builder.build();
  }

  public static OptionValue toOptionValue(OptionValueProto value) {
    switch (value.getOptionValCase()) {
      case NUM_VAL:
        return OptionValue.createLong(
            OptionValue.OptionType.SYSTEM, value.getName(), value.getNumVal());
      case STRING_VAL:
        return OptionValue.createString(
            OptionValue.OptionType.SYSTEM, value.getName(), value.getStringVal());
      case BOOL_VAL:
        return OptionValue.createBoolean(
            OptionValue.OptionType.SYSTEM, value.getName(), value.getBoolVal());
      case FLOAT_VAL:
        return OptionValue.createDouble(
            OptionValue.OptionType.SYSTEM, value.getName(), value.getFloatVal());
      case OPTIONVAL_NOT_SET:
      default:
        throw new IllegalArgumentException("Invalid OptionValue kind");
    }
  }

  public static OptionValueProtoList toOptionValueProtoList(
      Collection<OptionValueProto> optionValueProtos) {
    return OptionValueProtoList.newBuilder().addAllOptions(optionValueProtos).build();
  }
}
