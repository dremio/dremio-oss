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
import { extractValue } from '@app/reducers/reducerFactories';

describe('extractValue', () => {

  it('returns null as default value', () => {
    expect(extractValue('test', 'type')(undefined, { type: '_init' })).to.be.null;
  });

  const test = (actionType, action, pathToValue, expectedValue) => {
    it(`actionType to listen = '${actionType}'; action = ${JSON.stringify(action)};
        path to extract = '${pathToValue}'; expected state value = ${JSON.stringify(expectedValue)}`,
      () => {
        const reducer = extractValue(actionType, pathToValue);
        expect(reducer(undefined, action)).to.be.equal(expectedValue);
      });
  };

  const obj = { b: 1};

  [
    ['action1', { type: 'other_type'}, 'type', null],
    ['action1', { type: 'action1'}, 'type', 'action1'],
    ['action1', { type: 'action1', a: obj }, 'a', obj],
    ['action1', { type: 'action1', a: obj }, 'a.b', 1],
    ['action1', { type: 'action1', a: obj }, 'a.b.c', null],
    ['action1', { type: 'action1' }, 'a.b.c', null]
  ].forEach(args => test(...args));
});
