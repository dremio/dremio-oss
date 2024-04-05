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
package com.dremio.datastore.indexed.doughnut;

import com.dremio.datastore.Converter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;

/** Doughnut Converter to serialize and deserialize Doughnut objects. */
public class DoughnutConverter extends Converter<Doughnut, byte[]> {
  public DoughnutConverter() {}

  @Override
  public byte[] convert(Doughnut v) {
    try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutput out = new ObjectOutputStream(bos)) {
      out.writeObject(v);
      return bos.toByteArray();
    } catch (IOException e) {
      throw new RuntimeException("failed to serialize", e);
    }
  }

  @Override
  public Doughnut revert(byte[] v) {
    try (ByteArrayInputStream bis = new ByteArrayInputStream(v);
        ObjectInput in = new ObjectInputStream(bis)) {
      return (Doughnut) (in.readObject());
    } catch (IOException | ClassNotFoundException e) {
      throw new RuntimeException("failed to deserialize", e);
    }
  }
}
