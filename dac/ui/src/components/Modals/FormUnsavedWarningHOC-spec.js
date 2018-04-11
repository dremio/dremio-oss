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

import { wrapUnsavedChangesWarningWithModal } from './FormUnsavedWarningHOC';

describe('FormUnsavedWarningHOC', () => {

  const MockModalComponent = class extends Component {
    render() {
      return (<div>Fake Modal</div>);
    }
  };

  const TestComponent = wrapUnsavedChangesWarningWithModal(MockModalComponent);
  let minimalProps;
  beforeEach(() => {
    minimalProps = {
      hide: sinon.spy(),
      showUnsavedChangesConfirmDialog: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#updateFormDirtyState', () => {

    it('should set state', () => {
      const instance = shallow(<TestComponent {...minimalProps}/>).instance();
      instance.updateFormDirtyState(true);
      expect(instance.state.isFormDirty).to.be.true;
    });
  });

  describe('#handleHide', () => {
    it('should call hide from the props and reset form dirty state', () => {
      const instance = shallow(<TestComponent {...minimalProps}/>).instance();
      sinon.spy(instance, 'updateFormDirtyState');
      instance.handleHide();
      expect(minimalProps.hide).to.be.called;
      expect(instance.updateFormDirtyState).to.be.calledWith(false);
    });
  });

  describe('#hide', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<TestComponent {...minimalProps}/>).instance();
      sinon.spy(instance, 'handleHide');
    });

    it('should call handleHide when form not dirty', () => {
      instance.hide();
      expect(instance.handleHide).to.be.calledOnce;
    });

    it('should call handleHide when form dirty and submitted', () => {
      instance.setState({isFormDirty: true});
      instance.hide(null, true);
      expect(instance.handleHide).to.be.calledOnce;
    });

    it('should call showUnsavedChangesConfirmDialog when form is dirty', () => {
      instance.setState({isFormDirty: true});
      instance.hide();
      expect(minimalProps.showUnsavedChangesConfirmDialog).to.be.calledOnce;
    });
  });
});
