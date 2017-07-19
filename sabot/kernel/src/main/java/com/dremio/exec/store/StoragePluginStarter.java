/*
 * Copyright (C) 2017 Dremio Corporation
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
package com.dremio.exec.store;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import com.dremio.common.exceptions.ExecutionSetupException;
import com.dremio.common.store.StoragePluginConfig;
import com.dremio.exec.store.sys.PersistentStore;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;

/**
 * System responsible for doing a parallel start of plugins. Will also
 * continue execution after a defined timeout and report final startup result.
 */
class StoragePluginStarter {

  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(StoragePluginStarter.class);

  private final Vector<StartResult> results = new Vector<>();
  private final PersistentStore<StoragePluginConfig> pluginSystemTable;
  private final List<PluginCreationTask> creationList = new ArrayList<>();
  private volatile CountDownLatch latch;
  private final long timeoutInMillis;
  private final StoragePluginCreator creator;

  /**
   * Whether tasks self report status.
   */
  private volatile boolean selfReport = false;

  public StoragePluginStarter(
      PersistentStore<StoragePluginConfig> pluginSystemTable,
      long timeoutInMillis,
      StoragePluginCreator creator
      ) {
    this.pluginSystemTable = pluginSystemTable;
    this.timeoutInMillis = timeoutInMillis;
    this.creator = creator;
  }

  /**
   * Add a plugin to start. Can only be used until start() is called.
   * @param name The name of the plugin.
   * @param config The configuration of the plugin.
   */
  public void add(String name, StoragePluginConfig config){
    creationList.add(new PluginCreationTask(name, config));
  }

  /**
   * Starts the plugins
   * @return
   */
  public List<Success> start(){
    logger.info("Starting storage plugins.");
    final Stopwatch watch = Stopwatch.createStarted();
    latch = new CountDownLatch(creationList.size());
    for(PluginCreationTask c : creationList){
      c.start();
    }

    try {
      latch.await(timeoutInMillis, TimeUnit.MILLISECONDS);
    } catch (InterruptedException e) {
      throw Throwables.propagate(e);
    }

    // make sure no reports come in while interacting with result list.
    synchronized(StoragePluginStarter.this){
      selfReport = true;
    }

    Map<String, StartResult> results = FluentIterable.from(this.results).uniqueIndex(new Function<StartResult, String>(){
      @Override
      public String apply(StartResult input) {
        return input.name;
      }});

    int successes = FluentIterable.from(this.results).filter(new Predicate<StartResult>(){
      @Override
      public boolean apply(StartResult input) {
        return input.isSuccess();
      }}).size();

    int pending = creationList.size() - results.size();

    int failures = results.size() - successes;

    StringBuilder sb = new StringBuilder();

    List<Success> startedPlugins = new ArrayList<>();

    if(failures > 0){
      sb.append("Some storage plugins failed to start. Full report below.");
    } else if(pending > 0){
      sb.append("Some storage plugins are still pending, startup continuing without waiting.");
    } else {
      sb.append("Storage plugins startup successful.");
    }
    sb.append("\n\n");
    sb.append(String.format("\tTotal Time: %dms\n", watch.elapsed(TimeUnit.MILLISECONDS)));
    sb.append(String.format("\tSuccesses: %d\n", successes));
    sb.append(String.format("\tPending: %d\n", pending));
    sb.append(String.format("\tFailures: %d\n", failures));
    sb.append("\n");
    sb.append("Specific Plugin Results\n");

    for(PluginCreationTask plugin : creationList){
      String name = plugin.name;
      StartResult result = results.get(name);
      if(result == null){
        sb.append(String.format("\t%s: pending\n", name));
      }else {
        if(result.isSuccess()){
          sb.append(String.format("\t%s: success (%dms)\n", name, result.timeTakenMillis));
          startedPlugins.add((Success) result);
        } else {
          sb.append(String.format("\t%s: failed (%dms)\n", name, result.timeTakenMillis));
        }
      }
    }

    if(failures > 0){
      logger.error(sb.toString());
      for(StartResult result : this.results){
        if(!result.isSuccess()){
          logger.error("Failure starting plugin {} after {}ms.", result.name, result.timeTakenMillis, ((Failure) result).exception);
        }
      }
    } else if(pending > 0){
      logger.warn(sb.toString());
    } else {
      logger.info(sb.toString());
    }

    return startedPlugins;
  }

  /**
   * Individual storage subsystem startup task. We use Threads directly here
   * since this is a one time operation and we don't want to manage a pool if a
   * few stragglers are waiting beyond the startup time.
   */
  private class PluginCreationTask extends Thread {

    private final String name;
    private final StoragePluginConfig config;

    public PluginCreationTask(String name, StoragePluginConfig config) {
      setName(String.format("initial-connect-%s.", name));
      setDaemon(true);
      this.name = name;
      this.config = config;
    }

    @Override
    public void run() {
      StartResult result = null;
      StoragePlugin<?> plugin = null;
      final Stopwatch watch = Stopwatch.createStarted();
      try {

        // Remove once DX-7336 is completed. Delete bootstap plugins that are disabled.
        if(config.wasPreviouslyDisabled()){
          try {
            pluginSystemTable.delete(name);
            return;
          } catch(Exception e){
            logger.info("Failure while deleting previously disabled config.", e);
            return;
          }
        }

        try {
          plugin = creator.create(name, config);
          result = new Success(name, watch.elapsed(TimeUnit.MILLISECONDS), plugin);
        } catch (ExecutionSetupException e) {
          pluginSystemTable.put(name, config);
          result = new Failure(name, watch.elapsed(TimeUnit.MILLISECONDS), e);
        }

      } catch (Throwable ex){
        result = new Failure(name, watch.elapsed(TimeUnit.MILLISECONDS), ex);
      } finally {
        try {
          if(result == null){
            result = new Failure(name, watch.elapsed(TimeUnit.MILLISECONDS), new RuntimeException("Unknown exception."));
          }

          synchronized (this) {
            if(!selfReport){
              results.add(result);
              return;
            }
          }

          // the main thread has moved on, need to report status.
          if(result.isSuccess()){
            logger.info("Startup for plugin {} completed in {}ms.", result.name, result.timeTakenMillis);
            creator.informLateCreate(name, ((Success)result).plugin);
          } else {
            logger.info("Startup for plugin {} failed after {}ms.", result.name, result.timeTakenMillis, ((Failure) result).exception);
          }

        }finally {
          latch.countDown();
        }
      }
    }
  }

  public abstract static class StartResult {
    private final String name;
    private final long timeTakenMillis;

    private StartResult(String name, long timeTakenMillis) {
      super();
      this.name = name;
      this.timeTakenMillis = timeTakenMillis;
    }

    public String getName() {
      return name;
    }

    public long getTimeTakenMillis() {
      return timeTakenMillis;
    }

    abstract boolean isSuccess();

  }

  private static class Failure extends StartResult {
    private final Throwable exception;

    private Failure(String name, long timeTakenMillis, Throwable exception) {
      super(name, timeTakenMillis);
      this.exception = exception;
    }

    public boolean isSuccess(){
      return false;
    }
  }

  public static class Success extends StartResult {

    private final StoragePlugin<?> plugin;
    private Success(String name, long timeTakenMillis, StoragePlugin<?> plugin) {
      super(name, timeTakenMillis);
      this.plugin = plugin;
    }

    public boolean isSuccess(){
      return true;
    }

    public StoragePlugin<?> getPlugin() {
      return plugin;
    }


  }

  /**
   * Interface to allow a starter to interact with the StoragePluginRegistry
   */
  interface StoragePluginCreator {
    /**
     * Create a new plugin
     * @param name Name of the plugin.
     * @param pluginConfig The configuration for the plugin.
     * @return The plugin created.
     * @throws Exception
     */
    StoragePlugin<?> create(String name, StoragePluginConfig pluginConfig) throws Exception;

    /**
     * Notify caller that a plugin was created after the deadline.
     * @param name Name of plugin.
     * @param plugin Plugin created.
     */
    void informLateCreate(String name, StoragePlugin<?> plugin);
  }
}
