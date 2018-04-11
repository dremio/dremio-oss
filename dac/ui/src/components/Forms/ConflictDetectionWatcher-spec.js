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

import { wrapConflictDetectionForm } from './ConflictDetectionWatcher';

describe('ConflictDetectionWatcher', () => {
  const MockFormComponent = class extends Component {
    render() {
      return (<div>Fake Form</div>);
    }
  };

  let minimalProps;
  let commonProps;
  let TestComponent;
  beforeEach(() => {
    TestComponent = wrapConflictDetectionForm(MockFormComponent);
    minimalProps = {};
    commonProps = {
      version: 1,
      submitting: false,
      showConflictConfirmationDialog: sinon.spy(),
      getConflictedValues: (props) => props.version
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TestComponent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should not warn user about configuration conflict when getConflictedValues function is absent', () => {
    const wrapper = shallow(<TestComponent {...minimalProps} showConflictConfirmationDialog={sinon.spy()}/>);
    wrapper.setProps({values: {}});
    expect(wrapper.props().showConflictConfirmationDialog).to.be.not.called;
  });

  describe('#componentWillReceiveProps', () => {
    let wrapper;
    beforeEach(() => {
      wrapper = shallow(<TestComponent {...commonProps}/>);
    });

    it('should warn user about configuration conflict when version field has changed', () => {
      wrapper.setProps({version: 2});
      expect(commonProps.showConflictConfirmationDialog).to.be.called;
    });

    it('should not warn user about configuration conflict when version field has not changed', () => {
      wrapper.setProps(commonProps);
      expect(commonProps.showConflictConfirmationDialog).to.be.not.called;
    });

    it('should not perform any check when form is submitting', () => {
      wrapper.setProps({submitting: true});
      expect(commonProps.showConflictConfirmationDialog).to.be.not.called;
    });
  });

  describe('#isConflictedValueChanged', () => {
    let instance;
    beforeEach(() => {
      instance = shallow(<TestComponent {...commonProps}/>).instance();
    });

    it('should return false when one of the values is undefined', () => {
      expect(instance.isConflictedValueChanged({}, {version: 1})).to.be.false;
      expect(instance.isConflictedValueChanged({version: 1}, {})).to.be.false;
    });

    it('should return false when next and old values are equal', () => {
      expect(instance.isConflictedValueChanged({version: 1}, {version: 1})).to.be.false;
    });

    it('should return true when next and old values are different', () => {
      expect(instance.isConflictedValueChanged({version: 1}, {version: 2})).to.be.true;
    });
  });
});
