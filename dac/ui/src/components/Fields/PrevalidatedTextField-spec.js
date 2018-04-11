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
import Keys from 'constants/Keys.json';
import PrevalidatedTextField from './PrevalidatedTextField';

describe('PrevalidatedTextField', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      onChange: sinon.spy(),
      value: '123'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<PrevalidatedTextField {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render TextField component', () => {
    const wrapper = shallow(<PrevalidatedTextField {...commonProps}/>);
    expect(wrapper.find('TextField')).to.have.length(1);
  });

  describe('componentWillReceiveProps', () => {
    let nextProps;
    beforeEach(() => {
      nextProps = { value: '123' };
    });
    it('should set state with nextProps if internal value is empty string', () => {
      const props = {
        ...commonProps,
        value: ''
      };
      const wrapper = shallow(<PrevalidatedTextField {...props}/>);
      const instance = wrapper.instance();
      instance.componentWillReceiveProps(nextProps);
      expect(wrapper.state('internalValue')).to.equal(nextProps.value);
    });
    it('should set state with nextProps if value from props was changed', () => {
      const props = {
        ...commonProps,
        value: '1'
      };
      const wrapper = shallow(<PrevalidatedTextField {...props}/>);
      const instance = wrapper.instance();
      instance.componentWillReceiveProps(nextProps);
      expect(wrapper.state('internalValue')).to.equal(nextProps.value);
    });
  });

  describe('handleUpdateTextField', () => {
    it('should call onChange if validate is undefined', () => {
      const wrapper = shallow(<PrevalidatedTextField {...commonProps}/>);
      const instance = wrapper.instance();
      instance.handleUpdateTextField();
      expect(commonProps.onChange.called).to.be.true;
      expect(commonProps.onChange.calledWith(commonProps.value)).to.be.true;
    });
    it('should not call onChange if validate returns false and reset to previous value', () => {
      const wrapper = shallow(<PrevalidatedTextField {...commonProps}/>);
      wrapper.setProps({
        validate: sinon.stub()
          .withArgs('2').returns(false)
      });
      wrapper.setState({
        internalValue: '2'
      });
      const instance = wrapper.instance();
      instance.handleUpdateTextField();
      expect(commonProps.onChange.called).to.be.false;
      expect(wrapper.state('internalValue')).to.equal(commonProps.value);
    });
    it('should call onChange if validate returns true', () => {
      const wrapper = shallow(<PrevalidatedTextField {...commonProps}/>);
      const state = {
        internalValue: '1'
      };
      wrapper.setProps({
        validate: sinon.stub()
          .withArgs('1').returns(true)
      });
      wrapper.setState(state);
      const instance = wrapper.instance();
      instance.handleUpdateTextField();
      expect(commonProps.onChange.called).to.be.true;
      expect(commonProps.onChange.calledWith(state.internalValue)).to.be.true;
    });
  });

  describe('handleTextFieldKeyDown', () => {
    let wrapper;
    let instance;
    let event;
    beforeEach(() => {
      wrapper = shallow(<PrevalidatedTextField {...commonProps}/>);
      instance = wrapper.instance();
      event = {
        stopPropagation: sinon.spy()
      };
      sinon.stub(instance, 'handleUpdateTextField');
    });
    it('should call handleUpdateTextField if keyCode equal to enter', () => {
      instance.handleTextFieldKeyDown({
        ...event,
        keyCode: Keys.ENTER
      });
      expect(event.stopPropagation.called).to.be.true;
      expect(instance.handleUpdateTextField.called).to.be.true;
    });
    it('should not call handleUpdateTextField if keyCode is different than enter', () => {
      instance.handleTextFieldKeyDown({
        ...event,
        keyCode: Keys.TAB
      });

      expect(event.stopPropagation.called).to.be.false;
      expect(instance.handleUpdateTextField.called).to.be.false;
    });
  });
});
