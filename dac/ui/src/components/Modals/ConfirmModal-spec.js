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
import localStorageUtils from 'utils/storageUtils/localStorageUtils';

import ConfirmModal from './ConfirmModal';

describe('ConfirmModal', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      onConfirm: sinon.spy(),
      onCancel: sinon.spy(),
      title: 'title1',
      text: 'text1'
    };
    commonProps = {
      ...minimalProps,
      isOpen: true,
      confirmText: 'confirmText1'
    };
    sinon.stub(localStorageUtils, 'getCustomValue');
    sinon.stub(localStorageUtils, 'setCustomValue');
  });
  afterEach(() => {
    localStorageUtils.getCustomValue.restore();
    localStorageUtils.setCustomValue.restore();
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ConfirmModal {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<ConfirmModal {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Modal with common props', () => {
    const wrapper = shallow(<ConfirmModal {...commonProps}/>);
    expect(wrapper.find('Modal')).to.have.length(1);
  });

  it('should render ConfirmCancelFooter with common props', () => {
    const wrapper = shallow(<ConfirmModal {...commonProps}/>);
    expect(wrapper.find('ConfirmCancelFooter')).to.have.length(1);
  });

  it('should render Checkbox only if doNotAskAgainText and doNotAskAgainKey are defined', () => {
    const wrapper = shallow(<ConfirmModal {...commonProps}/>);
    expect(wrapper.find('Checkbox')).to.have.length(0);

    wrapper.setProps({
      doNotAskAgainText: 'Text'
    });
    expect(wrapper.find('Checkbox')).to.have.length(0);

    wrapper.setProps({
      doNotAskAgainText: undefined,
      doNotAskAgainKey: 'warningDisabled'
    });
    expect(wrapper.find('Checkbox')).to.have.length(0);

    wrapper.setProps({
      doNotAskAgainText: 'Text'
    });
    expect(wrapper.find('Checkbox')).to.have.length(1);
  });

  describe('prompt field', () => {
    it('should render TextField only if showPrompt', () => {
      const wrapper = shallow(<ConfirmModal {...commonProps}/>);
      expect(wrapper.find('TextField')).to.have.length(0);

      wrapper.setProps({
        showPrompt: true,
        promptFieldProps: {foo: true}
      });
      expect(wrapper.find('TextField')).to.have.length(1);
      expect(wrapper.find('TextField').first().props().foo).to.be.true;
    });

    it('should pass state.promptValue to props.onConfirm', () => {
      const wrapper = shallow(<ConfirmModal {...commonProps}/>);
      wrapper.setState({promptValue: 'theValue'});
      wrapper.instance().onConfirm();
      expect(commonProps.onConfirm).to.be.calledWith('theValue');
    });
  });


  describe('#componentWillMount', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<ConfirmModal {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should call onConfirm if doNotAskAgainKey is defined and stored value from localStorage is true', () => {
      wrapper.setProps({
        doNotAskAgainKey: 'warningDisabled'
      });
      localStorageUtils.getCustomValue.returns(true);
      instance.componentWillMount();
      expect(commonProps.onConfirm).to.have.been.called;
    });
    it('should not call onConfirm if doNotAskAgainKey is undefined or stored value from localStorage is false', () => {
      instance.componentWillMount();
      expect(commonProps.onConfirm).to.have.not.been.called;
    });
  });

  describe('#onConfirm', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<ConfirmModal {...commonProps}/>);
      instance = wrapper.instance();
    });
    it('should call onConfirm and not call localStorageUtils.setCustomValue if state.doNotAskAgain is false ' +
      'or doNotAskAgainKey is undefined', () => {
      instance.onConfirm();
      expect(commonProps.onConfirm).to.have.been.called;
      expect(localStorageUtils.setCustomValue).to.have.not.been.called;
    });
    it('should store value for doNotAskAgainKey if it is defined and state.doNotAskAgain is true', () => {
      wrapper.setProps({
        doNotAskAgainKey: 'warningDisabled'
      });
      wrapper.setState({
        doNotAskAgain: true
      });
      instance.onConfirm();
      expect(localStorageUtils.setCustomValue).to.have.been.called;
    });
  });
});
