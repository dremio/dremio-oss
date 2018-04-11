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
import { Component } from 'react';

import wrapSubmitValueMutator from './wrapSubmitValueMutator';
const TestComponent = wrapSubmitValueMutator((values) => {
  delete values.thingA;
}, Component);

describe('wrapSubmitValueMutator', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      handleSubmit: sinon.spy((cb) => cb({thingA: 'a', thingB: 'b'}))
    };
    commonProps = {
      ...minimalProps,
      randomProp: 'foo'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<TestComponent {...commonProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.props().randomProp).to.be.equal('foo');
  });

  it('should pass thru all props except handleSubmit', () => {
    const wrapper = shallow(<TestComponent {...commonProps}/>);
    expect(wrapper.props().randomProp).to.be.equal('foo');
    expect(wrapper.props().handleSubmit).to.be.equal(wrapper.instance().handleSubmit);
  });

  it('#handleSubmit()', () => {
    const instance = shallow(<TestComponent {...commonProps}/>).instance();
    const onSubmit = sinon.stub().returns('canary');
    const ret = instance.handleSubmit(onSubmit);
    expect(onSubmit.getCall(0).args).to.be.eql([{thingB: 'b'}]);
    expect(ret).to.be.eql('canary');
  });
});
