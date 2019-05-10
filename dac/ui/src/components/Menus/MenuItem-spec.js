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
import { shallow, mount } from 'enzyme';
import MenuItemMaterial from '@material-ui/core/MenuItem';
import MenuItem from './MenuItem';

describe('MenuItem', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps,
      children: 'node',
      onClick: sinon.spy()
    };
  });
  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<MenuItem {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
  it('should render MenuItemMaterial', () => {
    const wrapper = shallow(<MenuItem {...commonProps}/>);
    expect(wrapper.find(MenuItemMaterial)).to.have.length(1);
    expect(wrapper).to.have.length(1);
  });
  it('should render Popover if menuItems is defined', () => {
    const props = {
      ...commonProps,
      menuItems: ['item1', 'item2']
    };
    const wrapper = shallow(<MenuItem {...props}/>);
    wrapper.setState({
      open: true
    });
    expect(wrapper.find('Popper')).to.have.length(1);
  });
  it('should not render Popover if menuItems is not defined', () => {
    const wrapper = shallow(<MenuItem {...commonProps}/>);

    expect(wrapper.find('Popper')).to.have.length(0);
  });
  describe('#handleRequestOpen', () => {
    it('should set state.open to true', () => {
      const wrapper = shallow(<MenuItem {...commonProps}/>);
      const instance = wrapper.instance();
      instance.handleRequestOpen();

      expect(wrapper.state('open')).to.be.true;
    });
  });
  describe('#handleRequestClose', () => {
    it('should set state.open to false', () => {
      const wrapper = shallow(<MenuItem {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setState({
        open: false
      });
      instance.handleRequestClose();

      expect(wrapper.state('open')).to.be.false;
    });
  });
  describe('#handleMouseLeave', () => {
    let handleRequestCloseSpy;
    let shouldCloseStub;
    let instance;
    let clock;
    beforeEach(() => {
      instance = shallow(<MenuItem {...commonProps}/>).instance();
      handleRequestCloseSpy = sinon.spy(instance, 'handleRequestClose');
      shouldCloseStub = sinon.stub(instance, 'shouldClose');
      clock = sinon.useFakeTimers();
    });
    afterEach(() => {
      handleRequestCloseSpy.restore();
      shouldCloseStub.restore();
      clock.restore();
    });

    it('should call handleRequestClose if shouldClose returns true', () => {
      shouldCloseStub.returns(true);
      const dummyEvent = {
        target: 'node'
      };
      instance.handleMouseLeave(dummyEvent);
      expect(shouldCloseStub).to.be.calledWith(dummyEvent);
      expect(instance.state.delayedCloseRequest).to.not.be.null;
      clock.tick(100);
      expect(handleRequestCloseSpy).to.be.called;
    });
    it('should not call handleRequestClose if shouldClose returns false', () => {
      shouldCloseStub.returns(false);

      instance.handleMouseLeave();
      expect(handleRequestCloseSpy).to.be.not.called;
    });
  });

  describe('#shouldClose', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      const mountWithContext = (node) => mount(node);
      const props = {
        ...commonProps,
        menuItems: ['item1', 'item2']
      };
      wrapper = mountWithContext(<MenuItem {...props}/>);
      instance = wrapper.instance();
      sinon.spy(instance, 'shouldClose');
    });
    afterEach(() => {
      instance.shouldClose.restore();
    });
    it('should call shouldClose and return true if relatedTarget is not .sub-item', () => {
      instance.handleMouseLeave({});
      expect(instance.shouldClose).to.be.called;
      expect(instance.shouldClose).to.be.returned(true);
    });
  });
});
