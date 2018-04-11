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
import { minimalFormProps } from 'testUtil';
import exploreUtils from 'utils/explore/exploreUtils';
import { TransformForm } from './TransformForm';

describe('TransformForm', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      ...minimalFormProps(),
      onFormSubmit: sinon.stub().returns(Promise.resolve()),
      resetRecommendedTransforms: sinon.stub()
    };
    commonProps = {
      ...minimalProps,
      dataset: Immutable.fromJS({}),
      values: {foo: 1, activeCard: undefined, cards: [{bar: 1}]},
      dirty: false,
      valid: true,
      autoPreview: false,
      handleSubmit: fn => fn,
      loadTransformCardPreview: sinon.spy(),
      onValuesChange: sinon.spy(),
      debounceDelay: 0,
      viewState: Immutable.fromJS({})
    };

    wrapper = shallow(<TransformForm {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper.setProps(...minimalProps);
    expect(wrapper).to.have.length(1);
  });

  it('should render', () => {
    expect(wrapper.find('DefaultWizardFooter')).to.have.length(1);
  });

  it('should only autoPeek when it is set, and valid', () => {
    sinon.stub(instance, 'autoPeek');

    // call initially
    expect(instance.autoPeek.callCount).to.equal(0);

    const newValues = {...commonProps.values, foo: 2};
    wrapper.setProps({values: newValues});
    expect(instance.autoPeek.callCount).to.equal(1);

    // don't call when not valid
    wrapper.setProps({values: newValues, valid: false});
    expect(instance.autoPeek.callCount).to.equal(1);
  });

  describe('autoPeek', () => {
    it('should handle and ignore form validation errors in onFormSubmit', () => {
      wrapper.setProps({onFormSubmit: sinon.stub().returns(Promise.reject({_error: {}}))});

      return expect(instance.autoPeek()).to.be.fulfilled;
    });
    it('should not handle errors in onFormSubmit that are not validation errors', () => {
      wrapper.setProps({onFormSubmit: sinon.stub().returns(Promise.reject())});

      return expect(instance.autoPeek()).to.be.rejected;
    });
  });

  it('should flush autoPeek debounce for card-level changes', () => {
    wrapper = shallow(<TransformForm {...commonProps} debounceDelay={1}/>);
    instance = wrapper.instance();
    sinon.stub(instance.autoPeek, 'flush');

    // call initially
    expect(instance.autoPeek.flush.callCount).to.equal(0);

    const newValues = {...commonProps.values, activeCard: 0};
    wrapper.setProps({values: newValues});
    expect(instance.autoPeek.flush.callCount).to.equal(1);
  });

  it('should call loadTransformCardPreview when card values change', () => {
    wrapper.setProps({values: {...commonProps.values, activeCard: 0}});
    expect(commonProps.loadTransformCardPreview).to.have.not.been.called;

    wrapper.setProps({values: {...commonProps.values, activeCard: 0, cards: [{bar: 2}]}});
    expect(commonProps.loadTransformCardPreview).to.have.been.calledOnce;
  });

  it('should call loadTransformCardPreview on mount is transform does not load cards from server', () => {
    wrapper = shallow(<TransformForm {...commonProps} debounceDelay={1}/>);
    instance = wrapper.instance();

    sinon.stub(exploreUtils, 'needsToLoadCardFormValuesFromServer').returns(true);
    instance.componentDidMount();
    expect(commonProps.loadTransformCardPreview).not.to.have.been.called;

    exploreUtils.needsToLoadCardFormValuesFromServer.returns(false);
    instance.componentDidMount();
    expect(commonProps.loadTransformCardPreview).to.have.been.called;

    exploreUtils.needsToLoadCardFormValuesFromServer.restore();
  });

  it('should call onValuesChange when values changes', () => {
    const newValues = {...commonProps.values, foo: 2};
    wrapper.setProps({values: newValues, dirty: true});
    expect(commonProps.onValuesChange).to.have.been.called;
  });

  it('#componentWillUnmount', () => {
    wrapper = shallow(<TransformForm {...commonProps} debounceDelay={1}/>);
    instance = wrapper.instance();
    sinon.stub(instance.autoPeek, 'cancel');
    sinon.stub(instance.updateCard, 'cancel');

    instance.componentWillUnmount();

    expect(instance.autoPeek.cancel).to.have.been.called;
    expect(instance.updateCard.cancel).to.have.been.called;
    expect(minimalProps.resetRecommendedTransforms).to.have.been.called;
  });
});
