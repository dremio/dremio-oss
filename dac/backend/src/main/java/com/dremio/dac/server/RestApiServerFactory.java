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
package com.dremio.dac.server;

import com.dremio.common.scanner.persistence.ScanResult;
import com.dremio.dac.annotations.RestApiServer;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.glassfish.jersey.server.ResourceConfig;

/**
 * Creates {@link ResourceConfig} classes annotated with {@link RestApiServer}. If scan result
 * contains both base and derived classes with the same pathSpec, only the derived one is kept.
 */
public class RestApiServerFactory {
  private static final org.slf4j.Logger logger =
      org.slf4j.LoggerFactory.getLogger(RestApiServerFactory.class);

  private final DACConfig dacConfig;
  private final ScanResult scanResult;
  private final ImmutableSet<String> allowedTags;

  public RestApiServerFactory(DACConfig dacConfig, ScanResult scanResult, Set<String> allowedTags) {
    this.dacConfig = dacConfig;
    this.scanResult = scanResult;
    this.allowedTags = ImmutableSet.copyOf(allowedTags);
  }

  /** Creates and initializes {@link ResourceConfig}'s annotated with {@link RestApiServer}. */
  public ImmutableList<ResourceConfig> load() {
    ImmutableList.Builder<ResourceConfig> builder = ImmutableList.builder();
    if (scanResult.getScannedAnnotations().contains(RestApiServer.class.getName())) {
      logger.info("Getting classes annotated with {}", RestApiServer.class.getName());

      // Get annotated classes.
      Set<Class<?>> annotatedClasses =
          new HashSet<>(scanResult.getAnnotatedClasses(RestApiServer.class));

      // Filter classes: base ones in presence of derived, w/o allowed tags.
      for (Class<?> clazz : annotatedClasses.toArray(new Class<?>[0])) {
        // Remove classes without allowed tags.
        RestApiServer annotation = clazz.getAnnotation(RestApiServer.class);
        if (Arrays.stream(annotation.tags().split(",", -1)).noneMatch(allowedTags::contains)) {
          logger.info(
              "Removing {} as it doesn't have one of the allowed tags: {} among: {}",
              clazz,
              allowedTags,
              annotation.tags());
          annotatedClasses.remove(clazz);
        }

        // Remove base classes if derived one is present.
        Class<?> superClass = clazz.getSuperclass();
        while (superClass != null) {
          RestApiServer superClassAnnotation = superClass.getAnnotation(RestApiServer.class);
          if (superClassAnnotation != null
              && superClassAnnotation.pathSpec().equals(annotation.pathSpec())) {
            logger.info(
                "Removing {} as {} has the same pathSpec {}",
                superClass,
                clazz,
                annotation.pathSpec());
            annotatedClasses.remove(superClass);
          }
          superClass = superClass.getSuperclass();
        }
      }

      // Check for duplicates on path spec.
      Multimap<String, Class<?>> byPathSpec =
          annotatedClasses.stream()
              .collect(
                  ArrayListMultimap::create,
                  (map, clazz) -> {
                    map.put(clazz.getAnnotation(RestApiServer.class).pathSpec(), clazz);
                  },
                  ArrayListMultimap::putAll);
      for (Map.Entry<String, Collection<Class<?>>> entry : byPathSpec.asMap().entrySet()) {
        if (entry.getValue().size() > 1) {
          throw new RuntimeException(
              String.format(
                  "Path spec %s have more than one server class %s",
                  entry.getKey(),
                  entry.getValue().stream().map(Class::getName).collect(Collectors.joining(","))));
        }
      }

      // Create instances of the classes.
      Injector injector =
          Guice.createInjector(
              new AbstractModule() {
                @Override
                protected void configure() {
                  bind(ScanResult.class).toInstance(scanResult);
                  bind(DACConfig.class).toInstance(dacConfig);
                }
              });
      for (Class<?> clazz : annotatedClasses) {
        RestApiServer annotation = clazz.getAnnotation(RestApiServer.class);
        builder.add((ResourceConfig) injector.getInstance(clazz));
        logger.info("Created {} for pathSpec = {}", clazz, annotation.pathSpec());
      }
    } else {
      logger.info("{} was not scanned", RestApiServer.class.getName());
    }
    return builder.build();
  }
}
