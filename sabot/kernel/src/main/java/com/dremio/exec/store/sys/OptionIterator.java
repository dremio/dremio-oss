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
package com.dremio.exec.store.sys;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import com.dremio.exec.server.SabotContext;
import com.dremio.exec.server.options.SabotConfigIterable;
import com.dremio.options.OptionManager;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.Kind;
import com.dremio.options.OptionValue.OptionType;
import com.dremio.sabot.exec.context.OperatorContext;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

public class OptionIterator implements Iterator<Object> {
//  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(OptionIterator.class);

  enum Mode {
    BOOT, SYS_SESS, BOTH
  };

  private final OptionManager fragmentOptions;
  private final Iterator<OptionValue> mergedOptions;

  public OptionIterator(final SabotContext dbContext, OperatorContext context, Mode mode){
    final SabotConfigIterable configOptions = new SabotConfigIterable(context.getConfig());
    fragmentOptions = context.getOptions();
    final Iterator<OptionValue> optionList;
    switch(mode){
    case BOOT:
      optionList = configOptions.iterator();
      break;
    case SYS_SESS:
      optionList = fragmentOptions.iterator();
      break;
    default:
      optionList = Iterators.concat(configOptions.iterator(), fragmentOptions.iterator());
    }

    List<OptionValue> values = Lists.newArrayList(optionList);
    Collections.sort(values);
    mergedOptions = values.iterator();

  }

  @Override
  public boolean hasNext() {
    return mergedOptions.hasNext();
  }

  @Override
  public OptionValueWrapper next() {
    final OptionValue value = mergedOptions.next();
    final Status status;
    if (value.getType() == OptionType.BOOT) {
      status = Status.BOOT;
    } else {
      final OptionValue def = fragmentOptions.getValidator(value.getName()).getDefault();
      if (value.equalsIgnoreType(def)) {
        status = Status.DEFAULT;
        } else {
        status = Status.CHANGED;
        }
      }
    return new OptionValueWrapper(value.getName(), value.getKind(), value.getType(), value.getNumVal(), value.getStringVal(),
        value.getBoolVal(), value.getFloatVal(), status);
  }

  public static enum Status {
    BOOT, DEFAULT, CHANGED
  }

  /**
   * Wrapper class for OptionValue to add Status
   */
  public static class OptionValueWrapper {

    public final String name;
    public final Kind kind;
    public final OptionType type;
    public final Status status;
    public final Long num_val;
    public final String string_val;
    public final Boolean bool_val;
    public final Double float_val;

    public OptionValueWrapper(final String name, final Kind kind, final OptionType type, final Long num_val,
        final String string_val, final Boolean bool_val, final Double float_val,
        final Status status) {
      this.name = name;
      this.kind = kind;
      this.type = type;
      this.num_val = num_val;
      this.string_val = string_val;
      this.bool_val = bool_val;
      this.float_val = float_val;
      this.status = status;
    }
  }

  @Override
  public void remove() {
    throw new UnsupportedOperationException();
  }
}
