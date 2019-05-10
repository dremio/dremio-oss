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

import { wrapUnsavedChangesWithForm } from './FormUnsavedRouteLeave';

describe('FormUnsavedRouteLeave', () => {

  const MockControllerComponent = class extends Component {
    render() {
      return (<div>Fake Controller</div>);
    }
  };

  const TestComponent = wrapUnsavedChangesWithForm(MockControllerComponent);
  let minimalProps;
  let context;
  beforeEach(() => {
    context = {
      router: {
        push: sinon.spy(),
        setRouteLeaveHook: sinon.spy()
      }
    };
    minimalProps = {
      route: {},
      showUnsavedChangesConfirmDialog: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>, {context});
    expect(wrapper).to.have.length(1);
  });

  describe('#updateFormDirtyState', () => {

    it('should set state', () => {
      const instance = shallow(<TestComponent {...minimalProps}/>, {context}).instance();
      instance.updateFormDirtyState(true);
      expect(instance.state.isFormDirty).to.be.true;
    });
  });

  describe('#routeWillLeave', () => {
    let instance;

    beforeEach(() => {
      instance = shallow(<TestComponent {...minimalProps}/>, {context}).instance();
    });

    it('should return true when form not dirty', () => {
      expect(instance.routeWillLeave()).to.be.true;
    });

    it('should call hide when form dirty and submitted', () => {
      instance.setState({isFormDirty: true});
      expect(instance.routeWillLeave()).to.be.false;
    });

    it('should call showUnsavedChangesConfirmDialog when form is dirty', () => {
      instance.setState({isFormDirty: true});
      instance.routeWillLeave();
      expect(minimalProps.showUnsavedChangesConfirmDialog).to.be.calledOnce;
    });

    it('should not call showUnsavedChangesConfirmDialog when redirect reason is unauthorized', () => {
      instance.setState({isFormDirty: true});
      instance.routeWillLeave({search: 'abc&reason=401'});
      expect(minimalProps.showUnsavedChangesConfirmDialog).to.be.not.called;
    });
  });

  describe('#leaveConfirmed', () => {
    let instance;
    let clock;

    beforeEach(() => {
      clock = sinon.useFakeTimers();
      instance = shallow(<TestComponent {...minimalProps}/>, {context}).instance();
    });

    afterEach(() => {
      clock.restore();
    });

    it('ignoreUnsavedChanges should be true when modal confirmed', () => {
      instance.leaveConfirmed();
      expect(instance.state.ignoreUnsavedChanges).to.be.true;
    });

    it('should call router.push when modal confirmed', () => {
      instance.leaveConfirmed();
      clock.tick(0);
      expect(context.router.push).to.be.calledOnce;
    });
  });

  describe('#setChildDirtyState', () => {
    it('should update overall form state as pristine when all child forms are pristine', () => {
      const instance = shallow(<TestComponent {...minimalProps} />, {context}).instance();
      sinon.spy(instance, 'updateFormDirtyState');
      instance.setChildDirtyState('form1')(false);
      instance.setChildDirtyState('form2')(false);
      expect(instance.updateFormDirtyState).to.have.been.calledWith(false);
      expect(instance.childDirtyStates).to.be.eql({ form1: false, form2: false});
      expect(instance.state.isFormDirty).to.be.false;
    });

    it('should update overall form state as dirty when some child form is dirty', () => {
      const instance = shallow(<TestComponent {...minimalProps} />, {context}).instance();
      sinon.spy(instance, 'updateFormDirtyState');
      instance.setChildDirtyState('form1')(true);
      instance.setChildDirtyState('form2')(false);
      expect(instance.updateFormDirtyState).to.have.been.calledWith(true);
      expect(instance.childDirtyStates).to.be.eql({ form1: true, form2: false});
      expect(instance.state.isFormDirty).to.be.true;
    });
  });
});
