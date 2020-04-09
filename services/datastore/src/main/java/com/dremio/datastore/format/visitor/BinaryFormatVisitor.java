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
package com.dremio.datastore.format.visitor;

import com.dremio.datastore.Converter;
import com.dremio.datastore.DatastoreFatalException;
import com.dremio.datastore.FormatVisitor;
import com.dremio.datastore.format.Format;
import com.google.protobuf.Message;

/**
 * A visitor that visits Formats and indicates whether this Format is Binary.
 */
public class BinaryFormatVisitor implements FormatVisitor<Boolean> {
  @Override
  public Boolean visitStringFormat() throws DatastoreFatalException {
    return false;
  }

  @Override
  public Boolean visitUUIDFormat() throws DatastoreFatalException {
    return false;
  }

  @Override
  public Boolean visitByteFormat() throws DatastoreFatalException {
    return true;
  }

  @Override
  public <K1, K2> Boolean visitCompoundPairFormat(String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format) {
    return key1Format.apply(this) || key2Format.apply(this);
  }

  @Override
  public <K1, K2, K3> Boolean visitCompoundTripleFormat(String key1Name, Format<K1> key1Format, String key2Name, Format<K2> key2Format, String key3Name, Format<K3> key3Format) throws DatastoreFatalException {
    return key1Format.apply(this) || key2Format.apply(this) || key3Format.apply(this);
  }

  @Override
  public <OUTER, INNER> Boolean visitWrappedFormat(Class<OUTER> clazz, Converter<OUTER, INNER> converter, Format<INNER> inner) throws DatastoreFatalException {
    return inner.apply(this);
  }

  @Override
  public <P extends Message> Boolean visitProtobufFormat(Class<P> clazz) throws DatastoreFatalException {
    return false;
  }

  @Override
  public <P extends io.protostuff.Message<P>> Boolean visitProtostuffFormat(Class<P> clazz) throws DatastoreFatalException {
    return false;
  }
}
