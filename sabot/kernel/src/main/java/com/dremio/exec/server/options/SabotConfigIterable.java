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
package com.dremio.exec.server.options;

import java.util.Iterator;
import java.util.Map.Entry;


import com.dremio.common.config.SabotConfig;
import com.dremio.options.OptionValue;
import com.dremio.options.OptionValue.OptionType;
import com.typesafe.config.ConfigValue;

public class SabotConfigIterable implements Iterable<OptionValue> {
  static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SabotConfigIterable.class);

  SabotConfig c;
  public SabotConfigIterable(SabotConfig c){
    this.c = c;
  }

  @Override
  public Iterator<OptionValue> iterator() {
    return new Iter(c);
  }

  public class Iter implements Iterator<OptionValue>{

    Iterator<Entry<String, ConfigValue>> entries;
    public Iter(SabotConfig c){
      entries = c.entrySet().iterator();
    }
    @Override
    public boolean hasNext() {
      return entries.hasNext();
    }

    @Override
    public OptionValue next() {
      final Entry<String, ConfigValue> e = entries.next();
      final ConfigValue cv = e.getValue();
      final String name = e.getKey();
      OptionValue optionValue = null;
      switch(cv.valueType()) {
      case BOOLEAN:
        optionValue = OptionValue.createBoolean(OptionType.BOOT, name, (Boolean) cv.unwrapped());
        break;

      case LIST:
      case OBJECT:
      case STRING:
        optionValue = OptionValue.createString(OptionType.BOOT, name, cv.render());
        break;

      case NUMBER:
        optionValue = OptionValue.createLong(OptionType.BOOT, name, ((Number) cv.unwrapped()).longValue());
        break;

      case NULL:
        throw new IllegalStateException("Config value \"" + name + "\" has NULL type");
      }

      return optionValue;
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

}
