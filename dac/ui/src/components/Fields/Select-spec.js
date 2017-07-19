/*
 * Copyright (C) 2017 Dremio Corporation
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
import Select from './Select';
describe('Select', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      onChange: sinon.spy(),
      items: [
        { option: 'name', label: 'NAME' },
        { option: 'foo', label: 'FOO' },
        { option: 'bar', label: 'BAR' }
      ]
    };
    wrapper = shallow(<Select {...commonProps}/>);
    instance = wrapper.instance();
  });
  it('should render with minimal props without exploding', () => {
    const wrapperMinProps = shallow(<Select {...minimalProps}/>);
    expect(wrapperMinProps).to.have.length(1);
  });
  it('should render 3 SelectItem components, Menu, Popover', () => {
    expect(wrapper.find('SelectItem')).to.have.length(3);
    expect(wrapper.find('Popover')).to.have.length(1);
    expect(wrapper.find('Menu')).to.have.length(1);
  });

  describe('#getButtonLabel', () => {
    it('should return label based on passed value', () => {
      expect(instance.getButtonLabel('name')).to.eql('NAME');
      expect(instance.getButtonLabel('foo')).to.eql('FOO');
    });
    it('should return empty string if value does not exist', () => {
      expect(instance.getButtonLabel('asd')).to.eql('');
    });
  });

  describe('#handleChange', () => {
    it('should call handleRequestClose and onChange if it exists', () => {
      sinon.spy(instance, 'handleRequestClose');
      instance.handleChange(null, 'name');
      expect(instance.handleRequestClose).to.be.called;
      expect(commonProps.onChange).to.be.calledWith('name');
    });
  });

  describe('#valueForItem', () => {
    it('should return option property if it exists', () => {
      const item = {
        option: 'option',
        label: 'label'
      };
      expect(instance.valueForItem(item)).to.eql(item.option);
    });
    it('should return label property if it exists and option is not exists', () => {
      const item = {
        label: 'label'
      };
      expect(instance.valueForItem(item)).to.eql(item.label);
    });
    it('should return argument if it does not have option or label', () => {
      const item = {
        prop: 'label'
      };
      expect(instance.valueForItem(item)).to.eql(item);
    });
  });

  describe('#handleTouchTap', () => {
    it('should set open and anchorEl properties on components state', () => {
      const prevState = wrapper.state();
      const e = {
        preventDefault: sinon.spy(),
        currentTarget: { target: 't' }
      };
      instance.handleTouchTap(e);
      const nextState = wrapper.state();
      expect(e.preventDefault).to.be.called;
      expect(nextState.open).to.eql(!prevState.open);
      expect(nextState.anchorEl).to.eql(e.currentTarget);
    });

    it('should not open menu when disabled from props is true', () => {
      const prevState = wrapper.state();
      const e = {
        preventDefault: sinon.spy(),
        currentTarget: { target: 't' }
      };
      wrapper.setProps({disabled: true});
      instance.handleTouchTap(e);
      const nextState = wrapper.state();
      expect(e.preventDefault).to.be.called;
      expect(nextState.open).to.eql(prevState.open);
      expect(nextState.anchorEl).to.eql(undefined);
    });
  });

  describe('#handleRequestClose', () => {
    it('should reset open to false', () => {
      wrapper.setState({ open: true });
      instance.handleRequestClose();
      expect(wrapper.state('open')).to.be.false;
    });
  });
});
