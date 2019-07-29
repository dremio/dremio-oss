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
package com.dremio.plugins.elastic.planning.functions;

import org.apache.calcite.rex.RexVisitor;

public class FunctionRenderer {

  public static enum RenderMode {
    GROOVY, PAINLESS
  }

  private final boolean supportsV5Features;
  private final boolean scriptsEnabled;

  private final RenderMode mode;
  private final RexVisitor<FunctionRender> visitor;

  public FunctionRenderer(
      boolean supportsV5Features,
      boolean scriptsEnabled,
      RenderMode mode,
      RexVisitor<FunctionRender> visitor) {
    super();
    this.supportsV5Features = supportsV5Features;
    this.scriptsEnabled = scriptsEnabled;
    this.mode = mode;
    this.visitor = visitor;
  }

  public RenderMode getMode() {
    return mode;
  }

  public boolean isUsingPainless(){
    return mode == RenderMode.PAINLESS;
  }

  public boolean isSupportsV5Features(){
    return supportsV5Features;
  }

  public boolean isScriptsEnabled(){
    return scriptsEnabled;
  }

  public RexVisitor<FunctionRender> getVisitor() {
    return visitor;
  }
}
