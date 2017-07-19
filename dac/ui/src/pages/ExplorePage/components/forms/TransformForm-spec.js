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
import { minimalFormProps } from 'testUtil';
import exploreUtils from 'utils/explore/exploreUtils';
import { TransformForm } from './TransformForm';

describe('TransformForm', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      ...minimalFormProps(),
      onFormSubmit: sinon.spy(),
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
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TransformForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render', () => {
    const wrapper = shallow(<TransformForm {...commonProps}/>);
    expect(wrapper.find('DefaultWizardFooter')).to.have.length(1);
  });

  it('should only autoPeek when it is set, and valid', () => {
    const wrapper = shallow(<TransformForm {...commonProps}/>);
    const instance = wrapper.instance();
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

  it('should flush autoPeek debounce for card-level changes', () => {
    const wrapper = shallow(<TransformForm {...commonProps} debounceDelay={1} />);
    const instance = wrapper.instance();
    sinon.stub(instance.autoPeek, 'flush');

    // call initially
    expect(instance.autoPeek.flush.callCount).to.equal(0);

    const newValues = {...commonProps.values, activeCard: 0};
    wrapper.setProps({values: newValues});
    expect(instance.autoPeek.flush.callCount).to.equal(1);
  });

  it('should call loadTransformCardPreview when card values change', () => {
    const wrapper = shallow(<TransformForm {...commonProps} />);

    wrapper.setProps({values: {...commonProps.values, activeCard: 0}});
    expect(commonProps.loadTransformCardPreview).to.have.not.been.called;

    wrapper.setProps({values: {...commonProps.values, activeCard: 0, cards: [{bar: 2}]}});
    expect(commonProps.loadTransformCardPreview).to.have.been.calledOnce;
  });

  it('should call loadTransformCardPreview on mount is transform does not load cards from server', () => {
    const wrapper = shallow(<TransformForm {...commonProps} debounceDelay={1}/>);
    const instance = wrapper.instance();

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
    const wrapper = shallow(<TransformForm {...commonProps} dirty/>);
    wrapper.setProps({values: newValues});
    expect(commonProps.onValuesChange).to.have.been.called;
  });

  it('#componentWillUnmount', () => {
    const instance = shallow(<TransformForm {...commonProps} debounceDelay={1} />).instance();
    sinon.stub(instance.autoPeek, 'cancel');
    sinon.stub(instance.updateCard, 'cancel');

    instance.componentWillUnmount();

    expect(instance.autoPeek.cancel).to.have.been.called;
    expect(instance.updateCard.cancel).to.have.been.called;
    expect(minimalProps.resetRecommendedTransforms).to.have.been.called;
  });
});
