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
package com.dremio.common.config;

import java.time.Duration;
import java.time.Period;
import java.time.temporal.TemporalAmount;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigList;
import com.typesafe.config.ConfigMemorySize;
import com.typesafe.config.ConfigMergeable;
import com.typesafe.config.ConfigObject;
import com.typesafe.config.ConfigOrigin;
import com.typesafe.config.ConfigResolveOptions;
import com.typesafe.config.ConfigValue;

public abstract class NestedConfig implements Config {

  private final Config config;

  protected NestedConfig(Config config) {
    this.config = config;
  }

  protected Config getInnerConfig(){
    return config;
  }

  @Override
  public ConfigObject root() {
    return config.root();
  }

  @Override
  public ConfigOrigin origin() {
    return config.origin();
  }

  @Override
  public Config withFallback(ConfigMergeable other) {
    return config.withFallback(other);
  }

  @Override
  public Config resolve() {
    return config.resolve();
  }

  @Override
  public Config resolve(ConfigResolveOptions options) {
    return config.resolve(options);
  }

  @Override
  public void checkValid(Config reference, String... restrictToPaths) {
    config.checkValid(reference, restrictToPaths);
  }

  @Override
  public boolean hasPath(String path) {
    return config.hasPath(path);
  }

  @Override
  public boolean hasPathOrNull(String path) {
    return config.hasPathOrNull(path);
  }

  @Override
  public boolean isEmpty() {
    return config.isEmpty();
  }

  @Override
  public Set<Entry<String, ConfigValue>> entrySet() {
    return config.entrySet();
  }

  @Override
  public boolean getBoolean(String path) {
    return config.getBoolean(path);
  }

  @Override
  public Number getNumber(String path) {
    return config.getNumber(path);
  }

  @Override
  public int getInt(String path) {
    return config.getInt(path);
  }

  @Override
  public long getLong(String path) {
    return config.getLong(path);
  }

  @Override
  public double getDouble(String path) {
    return config.getDouble(path);
  }

  @Override
  public String getString(String path) {
    return config.getString(path);
  }

  @Override
  public ConfigObject getObject(String path) {
    return config.getObject(path);
  }

  @Override
  public Config getConfig(String path) {
    return config.getConfig(path);
  }

  @Override
  public Object getAnyRef(String path) {
    return config.getAnyRef(path);
  }

  @Override
  public ConfigValue getValue(String path) {
    return config.getValue(path);
  }

  @Override
  public Long getBytes(String path) {
    return config.getBytes(path);
  }

  @Override
  public Long getMilliseconds(String path) {
    return config.getMilliseconds(path);
  }

  @Override
  public Long getNanoseconds(String path) {
    return config.getNanoseconds(path);
  }

  @Override
  public ConfigList getList(String path) {
    return config.getList(path);
  }

  @Override
  public List<Boolean> getBooleanList(String path) {
    return config.getBooleanList(path);
  }

  @Override
  public List<Number> getNumberList(String path) {
    return config.getNumberList(path);
  }

  @Override
  public List<Integer> getIntList(String path) {
    return config.getIntList(path);
  }

  @Override
  public List<Long> getLongList(String path) {
    return config.getLongList(path);
  }

  @Override
  public List<Double> getDoubleList(String path) {
    return config.getDoubleList(path);
  }

  @Override
  public List<String> getStringList(String path) {
    return config.getStringList(path);
  }

  @Override
  public List<? extends ConfigObject> getObjectList(String path) {
    return config.getObjectList(path);
  }

  @Override
  public List<? extends Config> getConfigList(String path) {
    return config.getConfigList(path);
  }

  @Override
  public List<? extends Object> getAnyRefList(String path) {
    return config.getAnyRefList(path);
  }

  @Override
  public List<Long> getBytesList(String path) {
    return config.getBytesList(path);
  }

  @Deprecated
  @Override
  public List<Long> getMillisecondsList(String path) {
    return config.getMillisecondsList(path);
  }

  @Deprecated
  @Override
  public List<Long> getNanosecondsList(String path) {
    return config.getNanosecondsList(path);
  }

  @Override
  public Duration getDuration(String path) {
    return config.getDuration(path);
  }

  @Override
  public List<Duration> getDurationList(String path) {
    return config.getDurationList(path);
  }

  @Override
  public <T extends Enum<T>> T getEnum(Class<T> enumClass, String path) {
    return config.getEnum(enumClass, path);
  }

  @Override
  public <T extends Enum<T>> List<T> getEnumList(Class<T> enumClass, String path) {
    return config.getEnumList(enumClass, path);
  }

  @Override
  public boolean getIsNull(String path) {
    return config.getIsNull(path);
  }

  @Override
  public ConfigMemorySize getMemorySize(String path) {
    return config.getMemorySize(path);
  }

  @Override
  public List<ConfigMemorySize> getMemorySizeList(String path) {
    return config.getMemorySizeList(path);
  }

  @Override
  public Period getPeriod(String path) {
    return config.getPeriod(path);
  }

  @Override
  public TemporalAmount getTemporal(String path) {
    return config.getTemporal(path);
  }

  @Override
  public Config withOnlyPath(String path) {
    return config.withOnlyPath(path);
  }

  @Override
  public Config withoutPath(String path) {
    return config.withoutPath(path);
  }

  @Override
  public Config atPath(String path) {
    return config.atPath(path);
  }

  @Override
  public Config atKey(String key) {
    return config.atKey(key);
  }

  @Override
  public Config withValue(String path, ConfigValue value) {
    return config.withValue(path, value);
  }

  @Override
  public long getDuration(String arg0, TimeUnit arg1) {
    return config.getDuration(arg0, arg1);
  }

  @Override
  public List<Long> getDurationList(String arg0, TimeUnit arg1) {
    return config.getDurationList(arg0, arg1);
  }

  @Override
  public boolean isResolved() {
    return config.isResolved();
  }

  @Override
  public Config resolveWith(Config arg0, ConfigResolveOptions arg1) {
    return config.resolveWith(arg0, arg1);
  }

  @Override
  public Config resolveWith(Config arg0) {
    return config.resolveWith(arg0);
  }


}
