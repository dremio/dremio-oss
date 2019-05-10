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
import { Popover } from '@app/components/Popover';
import { SelectView } from './SelectView';
import { select as selectCls } from './SelectView.less';

const testClass = 'test-class';
const testSelector = `.${testClass}`;
const testContent = <div className={testClass}></div>;
describe('SelectView', () => {
  it('should render Popover', () => {
    expect(shallow(<SelectView />).find(Popover)).to.have.length(1);
  });

  describe('#props.content', () => {
    it('works fine with render props pattern', () => {
      const renderFn = sinon.stub().returns(testContent);
      const wrapper = shallow(<SelectView content={renderFn} />);
      const instance = wrapper.instance();
      expect(renderFn).to.be.calledWith({
        openDD: instance.openDD,
        closeDD: instance.closeDD,
        isOpen: instance.isOpen
      });
      expect(wrapper.find(testSelector)).to.have.length(1);
    });

    it('works fine with node element', () => {
      const wrapper = shallow(<SelectView content={testContent} />);
      expect(wrapper.find(testSelector)).to.have.length(1);
    });
  });

  describe('#props.children', () => {
    it('works fine with render props pattern', () => {
      const renderFn = sinon.stub().returns(testContent);
      const wrapper = shallow(<SelectView children={renderFn} />);
      const instance = wrapper.instance();
      expect(renderFn).to.be.calledWith({
        openDD: instance.openDD,
        closeDD: instance.closeDD,
        isOpen: instance.isOpen
      });
      expect(wrapper.find(testSelector)).to.have.length(1);
    });

    it('works fine with node element', () => {
      const wrapper = shallow(<SelectView children={testContent} />);
      expect(wrapper.find(testSelector)).to.have.length(1);
    });
  });

  it('should open a Popover on main content click', () => { // runs to long
    const wrapper = shallow(<SelectView />);
    const instance = wrapper.instance();

    // simulate a ref to avoid mounting
    instance.contentRef = { current: <div></div> };
    expect(!!wrapper.find(Popover).prop('anchorEl')).to.be.false;
    wrapper.find(`.${selectCls}`).simulate('click');
    expect(!!wrapper.find(Popover).prop('anchorEl')).to.be.true;

    // there is no way to simulate a popover's click away action. So lets call closeDD directly
    instance.closeDD();
    expect(!!wrapper.find(Popover).prop('anchorEl')).to.be.false;
  });

  it('a Popover should stay closed if SelectView is disabled', () => {
    const wrapper = shallow(<SelectView disabled />);
    const instance = wrapper.instance();

    // simulate a ref to avoid mounting
    instance.contentRef = { current: <div></div> };
    expect(!!wrapper.find(Popover).prop('anchorEl')).to.be.false;
    wrapper.find(`.${selectCls}`).simulate('click');
    expect(!!wrapper.find(Popover).prop('anchorEl')).to.be.false;
  });
});
