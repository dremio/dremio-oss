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
package com.dremio.service.autocomplete.nessie;

import org.apache.arrow.util.Preconditions;

/**
 * A ref under refs/tags/ namespace that points to an object of an arbitrary type (typically a tag points to either a tag or a commit object).
 * In contrast to a head, a tag is not updated by the commit command.
 * A Git tag has nothing to do with a Lisp tag (which would be called an object type in Git’s context).
 * A tag is most typically used to mark a particular point in the commit ancestry chain.
 */
public final class Tag extends NessieElement {
  private final String name;

  public Tag(String name, Hash hash) {
    super(hash);
    Preconditions.checkNotNull(name);

    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public NessieElementType getType() {
    return NessieElementType.TAG;
  }

  @Override
  public <R> R accept(NessieElementVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
