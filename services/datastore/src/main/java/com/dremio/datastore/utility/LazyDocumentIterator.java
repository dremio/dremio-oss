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
package com.dremio.datastore.utility;

import com.dremio.datastore.api.Document;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Lazy document iterator takes an iterable of documents and provides an iterator interface to
 * lazily access documents' values.
 */
public class LazyDocumentIterator<KEY_TYPE, VALUE_TYPE> implements Iterator<VALUE_TYPE> {
  private final Iterator<Document<KEY_TYPE, VALUE_TYPE>> docs;

  public LazyDocumentIterator(Iterable<Document<KEY_TYPE, VALUE_TYPE>> docs) {
    this.docs = (docs != null) ? docs.iterator() : Collections.emptyIterator();
  }

  @Override
  public boolean hasNext() {
    return docs.hasNext();
  }

  @Override
  public VALUE_TYPE next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    try {
      Document<KEY_TYPE, VALUE_TYPE> nextDoc = docs.next();
      if (nextDoc == null) {
        throw new NoSuchElementException("Next call returned no element (null)");
      }
      return nextDoc.getValue();
    } catch (NoSuchElementException e) {
      /* We checked that hasNext() returned true but still didn't receive the doc from the next() call.
        This is an unexpected state to be in, so thus throw an IllegalStateException.
      */
      throw new IllegalStateException("Iterator did not have a next document as claimed.", e);
    }
  }
}
