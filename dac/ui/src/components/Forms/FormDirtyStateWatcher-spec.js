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

import FormDirtyStateWatcher from './FormDirtyStateWatcher';

describe('FormDirtyStateWatcher', () => {

  const MockFormComponent = class extends Component {
    render() {
      return (<div>Fake Form</div>);
    }
  };

  const TestComponent = FormDirtyStateWatcher(MockFormComponent, ['arrayBar', 'foo']);
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      dirty: false,
      initialValuesForDirtyStateWatcher: { // added by connectComplexForm
        arrayBar: []
      },
      values: {
        arrayBar: []
      },
      handleSubmit: sinon.spy(),
      updateFormDirtyState: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should set state dirty to true when form is dirty', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    const instance = wrapper.instance();
    wrapper.setProps({
      dirty: true
    });
    expect(instance.state.dirty).to.be.true;
  });

  it('should call updateFormDirtyState with true when form is dirty', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    wrapper.setProps({
      dirty: true
    });
    expect(minimalProps.updateFormDirtyState.calledWith(true)).to.be.true;
  });

  it('should set state dirty to true when arrayBar value has changed', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    const instance = wrapper.instance();
    wrapper.setProps({
      values: {
        arrayBar: [{}]
      }
    });
    expect(instance.state.dirty).to.be.true;
  });

  describe('#areArrayFieldsDirty', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<TestComponent {...minimalProps}/>);
      instance = wrapper.instance();
    });

    it('should return true when value has been changed', () => {
      wrapper.setProps({
        values: {
          arrayBar: [{}]
        }
      });
      expect(instance.areArrayFieldsDirty(wrapper.props())).to.be.true;
    });

    it('should return false when value has not been changed', () => {
      expect(instance.areArrayFieldsDirty(wrapper.props())).to.be.false;
    });

    it('should ignore undefined fields when comparing', () => {
      wrapper = shallow(<TestComponent {...minimalProps} initialValuesForDirtyStateWatcher={{
        arrayBar: [{id: undefined, value: '1'}, {value: '2'}]
      }} />);
      wrapper.setProps({
        values: {
          arrayBar: [{value: '1'}, {id: undefined, value: '2'}]
        }
      });
      expect(wrapper.instance().areArrayFieldsDirty(wrapper.props())).to.be.false;
    });
  });

  describe('#_removeKeysWithUndefinedValue', () => {
    it('should replace undefined values inside objects, change undefined to null, and keep nonobjects the same', () => {
      const wrapper = shallow(<TestComponent {...minimalProps}/>);
      const instance = wrapper.instance();
      const valuesList = [
        {key1: 'foo'},
        {key2: undefined},
        null,
        1,
        undefined
      ];
      const expected = [{key1: 'foo'}, {}, null, 1, null];
      expect(instance._removeKeysWithUndefinedValue(valuesList)).to.be.eql(expected);
    });
  });
});
