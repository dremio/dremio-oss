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

import com.google.common.base.Preconditions;

/**
 * A "branch" is a line of development.
 * The most recent commit on a branch is referred to as the tip of that branch.
 * The tip of the branch is referenced by a branch head, which moves forward as additional development is done on the branch.
 * A single Git repository can track an arbitrary number of branches,
 * but your working tree is associated with just one of them (the "current" or "checked out" branch),
 * and HEAD points to that branch.
 */
public final class Branch extends NessieElement {
  private final String name;

  public Branch(String name, Hash hash) {
    super(hash);
    Preconditions.checkNotNull(name);
    this.name = name;
  }

  public String getName() {
    return name;
  }

  @Override
  public NessieElementType getType() {
    return NessieElementType.BRANCH;
  }

  @Override
  public <R> R accept(NessieElementVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
