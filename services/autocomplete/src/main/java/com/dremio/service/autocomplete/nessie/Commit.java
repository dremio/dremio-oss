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

import java.time.Instant;

import org.apache.arrow.util.Preconditions;

/**
 *  A single point in the Git history; the entire history of a project is represented as a set of interrelated commits.
 *  The word "commit" is often used by Git in the same places other revision control systems use the words "revision" or "version".
 *  Also used as a short hand for commit object.
 */
public final class Commit extends NessieElement {
  private final String authorName;
  private final Instant timestamp;
  private final String message;

  public Commit(Hash hash, String authorName, Instant timestamp, String message) {
    super(hash);
    Preconditions.checkNotNull(authorName);
    Preconditions.checkNotNull(timestamp);
    Preconditions.checkNotNull(message);

    this.authorName = authorName;
    this.timestamp = timestamp;
    this.message = message;
  }

  public String getAuthorName() {
    return authorName;
  }

  public Instant getTimestamp() {
    return timestamp;
  }

  public String getMessage() {
    return message;
  }

  @Override
  public NessieElementType getType() {
    return NessieElementType.COMMIT;
  }

  @Override
  public <R> R accept(NessieElementVisitor<R> visitor) {
    return visitor.visit(this);
  }
}
