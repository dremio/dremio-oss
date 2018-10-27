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
import { initModuleState, resetModuleState } from '@app/actions/modulesState';
import reducer, { getData } from './modulesState';

const fakeAction = 'ACTION';
const fakeReducer = (state = true, { type }) => {
  switch (type) {
  case fakeAction:
    return false;
  default:
    return state;
  }
};

const defaultState = reducer(undefined, {});
const key = 'module_key';
const initModule = () => reducer(defaultState, initModuleState(key, fakeReducer));

describe('modulesState', () => {
  it('getData returns null, if module was not initialized', () => {
    expect(getData(defaultState, key)).to.be.null;
  });

  (() => {
    let nextState = initModule();

    it('inits data correctly', () => {
      expect(getData(nextState, key)).to.be.true; // default value from fakeReducer is applied
    });

    // uses a state from previous test
    it('resets data correctly', () => {
      nextState = reducer(nextState, resetModuleState(key));
      expect(getData(nextState)).to.be.null;
    });
  })();

  it('updates underlied data correctly', () => {
    let nextState = initModule();
    nextState = reducer(nextState, { type: fakeAction });
    expect(getData(nextState, key)).to.be.false; // a value from fakeReducer in case of action
  });

  it('throws an error if different reducer is provided for the same key', () => {
    const nextState = initModule();

    expect(() => reducer(nextState, initModuleState(key, () => {}))).to.throw(); // error should be thrown in this case
  });
});
