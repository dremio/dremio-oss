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
import SelectWithPopover from './SelectWithPopover';

describe('SelectWithPopover', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      curItem: 'ds1'
    };
    commonProps = {
      ...minimalProps,
      onChange: sinon.spy(),
      items: [
        { value: 'name', label: 'NAME' },
        { value: 'foo', label: 'FOO' },
        { value: 'bar', label: 'BAR' }
      ]
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<SelectWithPopover {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render 3 MenuItem components, RaisedButton, Popover, Menu', () => {
    const wrapper = shallow(<SelectWithPopover {...commonProps}/>);
    expect(wrapper.find('RaisedButton')).to.have.length(1);
    expect(wrapper.find('Popover')).to.have.length(1);
    expect(wrapper.find('Menu')).to.have.length(1);
    expect(wrapper.find('MenuItem')).to.have.length(3);
  });

  describe('#renderItems', () => {
    it('should render MenuItems with value, primaryText', () => {
      const wrapper = shallow(<SelectWithPopover {...commonProps}/>);
      const instance = wrapper.instance();
      const items = instance.renderItems(commonProps.items);
      items.forEach((item, i) => {
        expect(item.props.value).to.eql(commonProps.items[i].value);
      });
    });
  });

  describe('#handleTouchTap', () => {
    it('should set open and anchorEl properties on components state', () => {
      const wrapper = shallow(<SelectWithPopover {...minimalProps}/>);
      const instance = wrapper.instance();
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
  });

  describe('#handleRequestClose', () => {
    it('should reset open to false', () => {
      const wrapper = shallow(<SelectWithPopover {...minimalProps}/>);
      const instance = wrapper.instance();
      wrapper.setState({ open: true });
      instance.handleRequestClose();
      expect(wrapper.state('open')).to.be.false;
    });
  });

  describe('#onChange', () => {
    it('should call handleRequestClose and onChange if it exists', () => {
      const instance = shallow(<SelectWithPopover {...commonProps}/>).instance();
      sinon.spy(instance, 'handleRequestClose');
      instance.onChange(null, 'name');
      expect(instance.handleRequestClose).to.be.called;
      expect(commonProps.onChange).to.be.calledWith('name');
    });
  });
});
