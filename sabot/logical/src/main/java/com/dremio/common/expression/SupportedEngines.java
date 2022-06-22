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

package com.dremio.common.expression;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

/**
 * Class to capture the supported execution engines
 * for an expression.
 * Supported engines are Gandiva and Java.
 */
public class SupportedEngines {
  public enum CodeGenOption {
    // use Gandiva only to generate code
    // This is a test-hook to be used by test code
    GandivaOnly,
    // prefer Java to generate code
    Java,
    // prefer Gandiva to generate code
    Gandiva;

    public static final CodeGenOption DEFAULT = Gandiva;

    // Converts optionName to an enum
    public static CodeGenOption getCodeGenOption(String optionName) {
      try {
        return CodeGenOption.valueOf(optionName);
      } catch (Exception e) {
        // The optionName may be the lower case string (e.g. java)
        for(CodeGenOption option : CodeGenOption.values()) {
          if (optionName.equalsIgnoreCase(option.toString())) {
            return option;
          }
        }
      }

      // unreachable code. The validator ensures that the value is one of the available choices
      return DEFAULT;
    }
  }

  public enum Engine {
    JAVA,
    GANDIVA
  }

  // creating static engine set to avoid large number of duplicate set in a very large code gen tree.
  private static final Set<Engine> BOTH_ENGINES = new HashSet<>(Arrays.asList(Engine.JAVA, Engine.GANDIVA));
  private static final Set<Engine> JAVA_ENGINE_ONLY = Collections.singleton(Engine.JAVA);
  private static final Set<Engine> GANDIVA_ENGINE_ONLY = Collections.singleton(Engine.GANDIVA);

  private enum EngineSet {
    EMPTY(Collections.emptySet()),
    BOTH(BOTH_ENGINES),
    JAVA(JAVA_ENGINE_ONLY),
    GANDIVA(GANDIVA_ENGINE_ONLY);

    private final Set<Engine> matchingEngineSet;
    private final int engineSetSize;
    private Function<Engine, EngineSet> addFunction;
    private Function<Engine, EngineSet> removeFunction;

    static {
      EMPTY.addFunction = (engine) -> engine == Engine.GANDIVA ? GANDIVA : JAVA;
      EMPTY.removeFunction = (engine) -> EMPTY;
      BOTH.addFunction = (engine) -> BOTH;
      BOTH.removeFunction = (engine) -> engine == Engine.GANDIVA ? JAVA : GANDIVA;
      JAVA.addFunction = (engine) -> engine == Engine.GANDIVA ? BOTH : JAVA;
      JAVA.removeFunction = (engine) -> engine == Engine.JAVA ? EMPTY : JAVA;
      GANDIVA.addFunction = (engine) -> engine == Engine.JAVA ? BOTH : GANDIVA;
      GANDIVA.removeFunction = (engine) -> engine == Engine.GANDIVA ? EMPTY : GANDIVA;
    }

    EngineSet(Set<Engine> engineSet) {
      this.matchingEngineSet = engineSet;
      this.engineSetSize = engineSet.size();
    }

    private Set<Engine> getMatchingEngineSet() {
      return matchingEngineSet;
    }

    private EngineSet onAdd(Engine engine) {
      return addFunction.apply(engine);
    }

    private EngineSet onRemove(Engine engine) {
      return removeFunction.apply(engine);
    }

    private int getEngineSetSize() {
      return engineSetSize;
    }
  }

  private EngineSet currentEngineSet;

  public SupportedEngines() {
    this.currentEngineSet = EngineSet.EMPTY;
  }

  private SupportedEngines(EngineSet currentEngineSet) {
    this.currentEngineSet = currentEngineSet;
  }

  public SupportedEngines duplicate() {
    return new SupportedEngines(currentEngineSet);
  }

  /**
   * Checks if an evaluation type is supported
   * @param engine Evaluation type to check
   * @return true if evaluation type is supported
   */
  public boolean contains(Engine engine) {
    return currentEngineSet.matchingEngineSet.contains(engine);
  }

  /**
   * Marks an execution engine as supported
   * @param engine Evaluation type to be added
   */
  public void add(Engine engine) {
    currentEngineSet = currentEngineSet.onAdd(engine);
  }

  /**
   * Removes an execution engine
   * @param engine Engine to be removed
   */
  public void remove(Engine engine) {
    currentEngineSet = currentEngineSet.onRemove(engine);
  }

  /**
   * Clears all supported engines.
   */
  public void clear() {
    currentEngineSet = EngineSet.EMPTY;
  }

  public boolean isEmpty() {
    return currentEngineSet.getMatchingEngineSet().isEmpty();
  }

  public Set<Engine> supportedEngines() {
    return currentEngineSet.getMatchingEngineSet();
  }

  public int size() {
    return currentEngineSet.getEngineSetSize();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SupportedEngines engines = (SupportedEngines) o;
    return currentEngineSet == engines.currentEngineSet;
  }

  @Override
  public int hashCode() {
    return Objects.hash(currentEngineSet);
  }
}
