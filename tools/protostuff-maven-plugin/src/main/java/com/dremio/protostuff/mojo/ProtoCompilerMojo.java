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

package com.dremio.protostuff.mojo;

import static org.apache.maven.plugins.annotations.ResolutionScope.COMPILE_PLUS_RUNTIME;

import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.Mojo;

/**
 * A Mojo mirror of Protostuff mojo, making execute thread-safe
 *
 */
@Mojo(name = "compile", configurator = "include-project-dependencies", requiresDependencyResolution = COMPILE_PLUS_RUNTIME, threadSafe = true)
public class ProtoCompilerMojo extends io.protostuff.mojo.ProtoCompilerMojo {

  private static final Object LOCK = new Object();

  @Override
  public void execute() throws MojoExecutionException, MojoFailureException {
    synchronized (LOCK) {
     super.execute();
    }
  }
}
