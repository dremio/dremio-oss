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
import { shallow } from 'enzyme';
import { KeyChangeTrigger } from './KeyChangeTrigger';

describe('KeyChangeTrigger', () => {
  let stub;
  let wrapper;
  const parameterToString = parameter => JSON.stringify(parameter, null, 2);
  const testChange = (oldValue, newValue, isOnChangeCalled) =>
    it(`${isOnChangeCalled ? 'calls' : 'does not call'} onChange if value changed from ${parameterToString(oldValue)} to ${parameterToString(newValue)}`, () => {
      stub = sinon.stub();
      wrapper = shallow(<KeyChangeTrigger keyValue={oldValue} onChange={stub} />, {
        disableLifecycleMethods: false
      });
      stub.resetHistory();
      wrapper.setProps({
        keyValue: newValue
      });
      if (isOnChangeCalled) {
        expect(stub).to.be.calledWith(newValue);
      } else {
        expect(stub).to.not.be.called;
      }
    });

  beforeEach(() => {
    stub = sinon.stub();
    wrapper = shallow(<KeyChangeTrigger keyValue='1' onChange={stub} />, {
      disableLifecycleMethods: false
    });
  });

  it('calls onChange on mount', () => {
    expect(stub).to.be.calledWith('1');
  });

  [
    //strings
    ['1', '2', true],
    ['1', '1', false],
    // numbers
    [1, 1, false],
    [1, 2, true],
    // boolean
    [false, false, false],
    [true, true, false],
    [true, false, true],
    [false, true, true],
    //object
    [{}, {}, false],
    [{ a: 1 }, { a: 1 }, false],
    [{}, { a : 1 }, true],
    [{}, { a : null }, true],
    [{}, { a : undefined }, true],
    //arrays
    [[], [], false],
    [[1], [1], false],
    [[1, 2, 3], [1, 2, 3], false],
    [[1], [1, 2], true],
    [[1, 2], [1], true],
    [[1, 2], [2, 1], true]
  ].map(args => testChange(...args));
});
