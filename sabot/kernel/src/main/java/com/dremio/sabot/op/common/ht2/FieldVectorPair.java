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
package com.dremio.sabot.op.common.ht2;

import java.util.Objects;
import org.apache.arrow.vector.FieldVector;

public class FieldVectorPair {
  private final FieldVector incoming;
  private final FieldVector outgoing;

  public FieldVectorPair(FieldVector incoming, FieldVector outgoing) {
    super();
    this.incoming = incoming;
    this.outgoing = outgoing;
  }

  public FieldVector getIncoming() {
    return incoming;
  }

  public FieldVector getOutgoing() {
    return outgoing;
  }

  @Override
  public int hashCode() {
    return Objects.hash(incoming, outgoing);
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
    FieldVectorPair other = (FieldVectorPair) obj;
    return Objects.equals(incoming, other.incoming) && Objects.equals(outgoing, other.outgoing);
  }
}
