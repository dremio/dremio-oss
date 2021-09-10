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
package com.dremio.exec.compile;

import java.io.IOException;

import org.codehaus.commons.compiler.CompileException;

import com.dremio.common.util.DremioStringUtils;
import com.dremio.common.util.FileUtils;
import com.dremio.exec.ExecConstants;
import com.dremio.exec.exception.ClassTransformationException;
import com.dremio.options.OptionManager;
import com.dremio.options.Options;
import com.google.common.base.Preconditions;

@Options
public class ClassTransformer {
  private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(ClassTransformer.class);

  private final OptionManager optionManager;

  public ClassTransformer(final OptionManager optionManager) {
    this.optionManager = optionManager;
  }

  public static class ClassSet {
    public final ClassSet parent;
    public final ClassNames precompiled;
    public final ClassNames generated;

    public ClassSet(ClassSet parent, String precompiled, String generated) {
      Preconditions.checkArgument(!generated.startsWith(precompiled),
          String.format("The new name of a class cannot start with the old name of a class, otherwise class renaming will cause problems. Precompiled class name %s. Generated class name %s",
              precompiled, generated));
      this.parent = parent;
      this.precompiled = new ClassNames(precompiled);
      this.generated = new ClassNames(generated);
    }

    public ClassSet getChild(String precompiled, String generated) {
      return new ClassSet(this, precompiled, generated);
    }

    public ClassSet getChild(String precompiled) {
      return new ClassSet(this, precompiled, precompiled.replace(this.precompiled.dot, this.generated.dot));
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((generated == null) ? 0 : generated.hashCode());
      result = prime * result + ((parent == null) ? 0 : parent.hashCode());
      result = prime * result + ((precompiled == null) ? 0 : precompiled.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ClassSet other = (ClassSet) obj;
      if (generated == null) {
        if (other.generated != null) {
          return false;
        }
      } else if (!generated.equals(other.generated)) {
        return false;
      }
      if (parent == null) {
        if (other.parent != null) {
          return false;
        }
      } else if (!parent.equals(other.parent)) {
        return false;
      }
      if (precompiled == null) {
        if (other.precompiled != null) {
          return false;
        }
      } else if (!precompiled.equals(other.precompiled)) {
        return false;
      }
      return true;
    }
  }

  public static class ClassNames {
    public final String dot;
    public final String slash;
    public final String clazz;

    public ClassNames(String className) {
      dot = className;
      slash = className.replace('.', FileUtils.separatorChar);
      clazz = FileUtils.separatorChar + slash + ".class";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((clazz == null) ? 0 : clazz.hashCode());
      result = prime * result + ((dot == null) ? 0 : dot.hashCode());
      result = prime * result + ((slash == null) ? 0 : slash.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      ClassNames other = (ClassNames) obj;
      if (clazz == null) {
        if (other.clazz != null) {
          return false;
        }
      } else if (!clazz.equals(other.clazz)) {
        return false;
      }
      if (dot == null) {
        if (other.dot != null) {
          return false;
        }
      } else if (!dot.equals(other.dot)) {
        return false;
      }
      if (slash == null) {
        if (other.slash != null) {
          return false;
        }
      } else if (!slash.equals(other.slash)) {
        return false;
      }
      return true;
    }
  }

  public Class<?> getImplementationClass(
      final QueryClassLoader classLoader,
      final TemplateClassDefinition<?> templateDefinition,
      final String entireClass,
      final String materializedClassName) throws ClassTransformationException {
    return getExtendedImplementationClass(classLoader, templateDefinition, entireClass, materializedClassName);
  }

  private Class<?> getExtendedImplementationClass(
      final QueryClassLoader classLoader,
      final TemplateClassDefinition<?> templateDefinition,
      final String entireClass,
      final String materializedClassName) throws ClassTransformationException {

    try {
      final long t1 = System.nanoTime();
      final ClassSet set = new ClassSet(null, templateDefinition.getTemplateClassName(), materializedClassName);
      final ClassBytes[] implementationClasses = classLoader.getClassByteCode(set.generated, entireClass);

      long totalBytecodeSize = 0;
      for (ClassBytes clazz : implementationClasses) {
        totalBytecodeSize += clazz.getBytes().length;
        classLoader.injectByteCode(clazz.getName(), clazz.getBytes());
      }

      Class<?> c = classLoader.findClass(set.generated.dot);
      if (templateDefinition.getExternalInterface().isAssignableFrom(c)) {
        if (logger.isDebugEnabled()) {
          logger.debug("Done compiling (bytecode size={}, time:{} millis).", DremioStringUtils.readable(totalBytecodeSize), (System.nanoTime() - t1) / 1000000);
        }
        return c;
      }

      throw new ClassTransformationException("The requested class did not implement the expected interface.");
    } catch (CompileException | IOException | ClassNotFoundException e) {
      if (optionManager.getOption(ExecConstants.JAVA_CODE_DUMP)) {
        logger.info(String.format("Failure generating transformation classes for value: \n %s", entireClass));
      }
      throw new ClassTransformationException("Failure generating transformation classes.", e);
    }
  }

}
