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
package com.dremio.exec.store.dfs;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import com.dremio.common.exceptions.UserException;
import com.dremio.common.logical.FormatPluginConfig;
import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.common.util.ConstructorChecker;
import com.dremio.exec.server.SabotContext;
import com.dremio.exec.store.deltalake.DeltaLakeFormatConfig;
import com.dremio.exec.store.easy.arrow.ArrowFormatPluginConfig;
import com.dremio.exec.store.easy.json.JSONFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin;
import com.dremio.exec.store.easy.text.TextFormatPlugin.TextFormatConfig;
import com.dremio.exec.store.iceberg.IcebergFormatConfig;
import com.dremio.exec.store.parquet.ParquetFormatConfig;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 * Responsible for instantiating format plugins
 */
public class FormatCreator {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(FormatCreator.class);

  private static final ConstructorChecker FORMAT_BASED = new ConstructorChecker(String.class, SabotContext.class, FormatPluginConfig.class, FileSystemPlugin.class);
  private static final ConstructorChecker DEFAULT_BASED = new ConstructorChecker(String.class, SabotContext.class, FileSystemPlugin.class);

  /**
   * Returns a Map from the FormatPlugin Config class to the constructor of the format plugin that accepts it.
   * This is used to create a format plugin instance from its configuration.
   * @param pluginClasses the FormatPlugin classes to index on their config class
   * @return a map of type to constructor that taks the config
   */
  private static Map<Class<?>, Constructor<?>> initConfigConstructors(Collection<Class<? extends FormatPlugin>> pluginClasses) {
    Map<Class<?>, Constructor<?>> constructors = Maps.newHashMap();
    for (Class<? extends FormatPlugin> pluginClass: pluginClasses) {
      for (Constructor<?> c : pluginClass.getConstructors()) {
        try {
          if (!FORMAT_BASED.check(c)) {
            continue;
          }
          Class<?> configClass = c.getParameterTypes()[2];
          constructors.put(configClass, c);
        } catch (Exception e) {
          logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
        }
      }
    }
    return constructors;
  }


  private final SabotContext context;
  private final FileSystemConf<?, ?> storageConfig;
  private final FileSystemPlugin<?> fsPlugin;

  /** format plugins initialized from the Sabot config, indexed by name */
  private final Map<String, FormatPlugin> pluginsByName;

  /** format plugins initialized from the Sabot config, indexed by {@link FormatPluginConfig} */
  private Map<FormatPluginConfig, FormatPlugin> pluginsByConfig;

  /** FormatMatchers for all configured plugins */
  private List<FormatMatcher> formatMatchers;

  /** FormatMatchers for "layer formats" which can potentially contain files of many formats. */
  private List<FormatMatcher> layeredFormatMatchers;

  /** The format plugin classes retrieved from classpath scanning */
  private final Collection<Class<? extends FormatPlugin>> pluginClasses;
  /** a Map from the FormatPlugin Config class to the constructor of the format plugin that accepts it.*/
  private final Map<Class<?>, Constructor<?>> configConstructors;

  public static Map<String, FormatPluginConfig> getDefaultFormats() {
    Map<String, FormatPluginConfig> defaultFormats = new TreeMap<>();
    defaultFormats.put("csv", createTextFormatPlugin(false, ',', Lists.newArrayList("csv")));
    defaultFormats.put("csvh", createTextFormatPlugin(true, ',', Lists.newArrayList("csvh")));
    defaultFormats.put("tsv", createTextFormatPlugin(false, '\t', Lists.newArrayList("tsv")));
    defaultFormats.put("psv", createTextFormatPlugin(false, '|', Lists.newArrayList("psv", "tbl")));
    defaultFormats.put("txt", createTextFormatPlugin(false, '\u0000', Lists.newArrayList("txt")));
    TextFormatConfig psva = createTextFormatPlugin(false, '|', Lists.newArrayList("psva", "tbla"));
    psva.autoGenerateColumnNames = true;
    defaultFormats.put("psva", psva);

    defaultFormats.put("parquet", new ParquetFormatConfig());
    defaultFormats.put("json", new JSONFormatPlugin.JSONFormatConfig());
    defaultFormats.put("dremarrow1", new ArrowFormatPluginConfig());
    defaultFormats.put("iceberg", new IcebergFormatConfig());
    defaultFormats.put("delta", new DeltaLakeFormatConfig());
    return defaultFormats;
  }

  /**
   * Creates a {@link TextFormatPlugin.TextFormatConfig}.
   *
   * Dremio populates the default values from a config file read by Jackson,
   * so the TextFormatConfig class doesn't have a useful constructor.
   *
   * @return - a new TextFormatConfig
   */
  public static TextFormatPlugin.TextFormatConfig createTextFormatPlugin(boolean extractHeader,
      char fieldDelimiter,
      List<String> extensions) {
    TextFormatPlugin.TextFormatConfig newText = new TextFormatPlugin.TextFormatConfig();
    newText.extractHeader = extractHeader;
    newText.fieldDelimiter = fieldDelimiter;
    newText.extensions = extensions;
    // Use the default values for all other fields for now
    return newText;
  }


  FormatCreator(
      SabotContext context,
      FileSystemConf<?, ?> storageConfig,
      ScanResult classpathScan,
      FileSystemPlugin fsPlugin) {
    this.context = context;
    this.storageConfig = storageConfig;
    this.fsPlugin = fsPlugin;
    this.pluginClasses = classpathScan.getImplementations(FormatPlugin.class);
    this.configConstructors = initConfigConstructors(pluginClasses);
    Map<String, FormatPlugin> pluginsByName = Maps.newHashMap();
    Map<FormatPluginConfig, FormatPlugin> pluginsByConfig = Maps.newHashMap();
    List<FormatMatcher> formatMatchers = Lists.newArrayList();
    List<FormatMatcher> layeredFormatMatchers = Lists.newArrayList();


    final Map<String, FormatPluginConfig> formats = getDefaultFormats();
    if (formats != null && !formats.isEmpty()) {
      for (Map.Entry<String, FormatPluginConfig> e : formats.entrySet()) {
        Constructor<?> c = configConstructors.get(e.getValue().getClass());
        if (c == null) {
          logger.warn("Unable to find constructor for storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName());
          continue;
        }
        try {
          FormatPlugin formatPlugin = (FormatPlugin) c.newInstance(e.getKey(), context, e.getValue(), fsPlugin);
          pluginsByName.put(e.getKey(), formatPlugin);
          pluginsByConfig.put(formatPlugin.getConfig(), formatPlugin);

          if (formatPlugin.isLayered()) {
            layeredFormatMatchers.add(formatPlugin.getMatcher());
            // add the layer ones at the top, so that they get checked first.
            formatMatchers.add(0, formatPlugin.getMatcher());
          } else {
            formatMatchers.add(formatPlugin.getMatcher());
          }
        } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e1) {
          logger.warn("Failure initializing storage config named '{}' of type '{}'.", e.getKey(), e.getValue().getClass().getName(), e1);
        }
      }
    }
    // Fall back to default constructor based initialization.
    for (Class<? extends FormatPlugin> pluginClass : pluginClasses) {
      for (Constructor<?> c : pluginClass.getConstructors()) {
        try {
          if (!DEFAULT_BASED.check(c)) {
            continue;
          }
          FormatPlugin plugin = (FormatPlugin) c.newInstance(null, context, fsPlugin);
          if (pluginsByName.containsKey(plugin.getName())) {
            continue;
          }
          pluginsByName.put(plugin.getName(), plugin);
          pluginsByConfig.put(plugin.getConfig(), plugin);

          if (plugin.isLayered()) {
            layeredFormatMatchers.add(plugin.getMatcher());
            // add the layer ones at the top, so that they get checked first.
            formatMatchers.add(0, plugin.getMatcher());
          } else {
            formatMatchers.add(plugin.getMatcher());
          }
        } catch (Exception e) {
          logger.warn(String.format("Failure while trying instantiate FormatPlugin %s.", pluginClass.getName()), e);
        }
      }
    }
    this.pluginsByName = Collections.unmodifiableMap(pluginsByName);
    this.pluginsByConfig = Collections.unmodifiableMap(pluginsByConfig);
    this.formatMatchers = Collections.unmodifiableList(formatMatchers);
    this.layeredFormatMatchers = Collections.unmodifiableList(layeredFormatMatchers);
  }

  public FileSystemPlugin getPlugin(){
    return fsPlugin;
  }

  /**
   * @param name the name of the formatplugin instance in the Sabot config
   * @return The configured FormatPlugin for this name
   */
  public FormatPlugin getFormatPluginByName(String name) {
    return pluginsByName.get(name);
  }

  /**
   * @param formatConfig {@link FormatPluginConfig} of the format plugin
   * @return The configured FormatPlugin for the given format config.
   */
  public FormatPlugin getFormatPluginByConfig(FormatPluginConfig formatConfig) {
    if (formatConfig instanceof NamedFormatPluginConfig) {
      return getFormatPluginByName(((NamedFormatPluginConfig) formatConfig).name);
    } else {
      return pluginsByConfig.get(formatConfig);
    }
  }

  /**
   * @return List of format matchers for all configured format plugins.
   */
  public List<FormatMatcher> getFormatMatchers() {
    return formatMatchers;
  }

  /**
   * @return List of format matchers for all configured layer format plugins.
   */
  public List<FormatMatcher> getLayeredFormatMatchers() {
    return layeredFormatMatchers;
  }

  /**
   * @return all the format plugins from the Sabot config
   */
  public Collection<FormatPlugin> getConfiguredFormatPlugins() {
    return pluginsByName.values();
  }

  /**
   * Instantiate a new format plugin instance from the provided config object
   * @param fpconfig the conf for the plugin
   * @return the newly created instance of a FormatPlugin based on provided config
   */
  public FormatPlugin newFormatPlugin(FormatPluginConfig fpconfig) {
    Constructor<?> c = configConstructors.get(fpconfig.getClass());
    if (c == null) {
      throw UserException.dataReadError()
        .message(
            "Unable to find constructor for storage config of type %s",
            fpconfig.getClass().getName())
        .build(logger);
    }
    try {
      return (FormatPlugin) c.newInstance(null, context, fpconfig, fsPlugin);
    } catch (InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw UserException.dataReadError(e)
        .message(
            "Failure initializing storage config of type %s",
            fpconfig.getClass().getName())
        .build(logger);
    }
  }
}
