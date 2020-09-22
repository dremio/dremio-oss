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

package org.apache.arrow.vector.holders;

/*
 * Holder class for the vector ObjectVector. This holder internally stores a
 * reference to an object. The ObjectVector maintains an array of these objects.
 * This holder can be used only as workspace variables in aggregate functions.
 * Using this holder should be avoided and we should stick to native holder types.
 */
@Deprecated
public class ObjectHolder implements ValueHolder {
  public Object obj;
}

