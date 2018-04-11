/*
 * Copyright (C) 2017-2018 Dremio Corporation
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
import Immutable  from 'immutable';

class StateUtils {
  _checkType(state, propertyPath) {
    if (!Immutable.Map.isMap(state)) {
      throw new Error('value supplied to request must be instance of Immutable.Map');
    }
    if (!Array.isArray(propertyPath)) {
      throw new Error('propertyPath must be Array check setIn method in Immutable Docs');
    }
  }

  request(state, propertyPath) {
    this._checkType(state, propertyPath);
    const activePath = propertyPath.slice(0, -1);
    return state.setIn([...activePath, 'isInProgress'], true).setIn([...activePath, 'isFailed'], false);
  }

  success(state, propertyPath, payload, mapFunction) {
    this._checkType(state, propertyPath);
    const activePath = propertyPath.slice(0, -1);
    return state.setIn(propertyPath, Immutable.fromJS(mapFunction(payload)))
      .setIn([...activePath, 'isInProgress'], false)
      .setIn([...activePath, 'isFailed'], false);
  }

  failed(state, propertyPath) {
    this._checkType(state, propertyPath);
    const activePath = propertyPath.slice(0, -1);
    return state.set([...activePath, 'isInProgress'], false).set([...activePath, 'isFailed'], true);
  }
}

const stateUtils = new StateUtils();

export default stateUtils;
