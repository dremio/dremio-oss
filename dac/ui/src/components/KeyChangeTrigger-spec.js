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
import { shallow } from 'enzyme';
import { KeyChangeTrigger } from './KeyChangeTrigger';

describe('KeyChangeTrigger', () => {
  let stub;
  let wrapper;
  beforeEach(() => {
    stub = sinon.stub();
    wrapper = shallow(<KeyChangeTrigger keyValue='1' onChange={stub} />, {
      disableLifecycleMethods: false
    });
  });

  it('calls onChange on mount', () => {
    expect(stub).to.be.calledWith('1');
  });

  it('calls onChange if key value is changed', () => {
    stub.reset();
    wrapper.setProps({
      keyValue: '2'
    });
    expect(stub).to.be.calledWith('2');
  });

  it('does not call onChange if key value is not changed', () => {
    stub.reset();
    wrapper.setProps({
      keyValue: '1'
    });
    expect(stub).have.not.been.called;
  });
});
