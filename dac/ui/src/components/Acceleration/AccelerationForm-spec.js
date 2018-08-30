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
import Immutable from 'immutable';
import { formFields } from 'testUtil';
import { cloneDeep } from 'lodash/lang';

import { createReflectionFormValues } from 'utils/accelerationUtils';


import { FormBody, ModalForm } from 'components/Forms';
//import Button from '../Buttons/Button';
import { AccelerationForm } from './AccelerationForm';

describe('AccelerationForm', () => {
  let minimalProps;
  let commonProps;
  let values;

  beforeEach(() => {
    values = {
      version: 1,
      aggregationReflections: [
        // WARNING: this might not be exactly accurate - but it's enough for the test
        createReflectionFormValues({id:'a', measureFields: [{name: 'cm1', measureTypeList: ['SUM', 'COUNT']}], dimensionFields: ['cd1'], type: 'AGGREGATION', tag: 'a', enabled: true}),
        createReflectionFormValues({id:'c', measureFields: [{name: 'cm1', measureTypeList: ['SUM', 'COUNT']}], dimensionFields: ['cd1'], type: 'AGGREGATION', tag: null, enabled: true})
      ],
      rawReflections: [
        // WARNING: this might not be exactly accurate - but it's enough for the test
        createReflectionFormValues({id:'b', displayFields: ['cm1'], type: 'RAW', tag: 'a', enabled: true}),
        createReflectionFormValues({id:'d', displayFields: ['cm1'], type: 'RAW', tag: null, enabled: true})
      ],
      columnsDimensions: [{column: 'cd1'}],
      columnsMeasures: [{column: 'cm1'}]
    };

    minimalProps = {
      handleSubmit: sinon.stub().returns(sinon.spy()),
      onSubmitSuccess: sinon.spy(),
      onCancel: sinon.spy(),
      location: {},
      dataset: Immutable.Map(),
      reflections: Immutable.Map(),
      values,
      fields: formFields(values),
      updateFormDirtyState: sinon.stub(),

      putReflection: sinon.stub().resolves(),
      postReflection: sinon.stub().resolves(),
      deleteReflection: sinon.stub().resolves(),
      lostFieldsByReflection: {}
    };
    commonProps = {
      showConfirmationDialog: sinon.stub(),
      fullPath: '',
      location: {},
      errors: {},
      submitFailed: false,
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<AccelerationForm {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render ModalForm with common props', () => {
    const wrapper = shallow(<AccelerationForm {...commonProps}/>);
    expect(wrapper.find(ModalForm)).to.have.length(1);
  });

  it('should render FormBody with common props', () => {
    const wrapper = shallow(<AccelerationForm {...commonProps}/>);
    expect(wrapper.find(FormBody)).to.have.length(1);
  });

  describe('#prepare', function() {
    let props;
    let expectedValues;
    beforeEach(() => {
      props = {
        ...commonProps
      };
      expectedValues = [...values.aggregationReflections, ...values.rawReflections];
    });

    it('should remove disabled RAW in BASIC (only)', function() {
      values.rawReflections[0].enabled = false;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      let result = instance.prepare(cloneDeep(values));
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});

      instance.setState({
        mode: 'BASIC'
      });
      result = instance.prepare(cloneDeep(values));
      values.rawReflections[0].shouldDelete = true;
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});
    });

    it('should remove disabled AGGREGATION in BASIC (only) if unconfigured', function() {
      values.aggregationReflections[0].enabled = false;
      values.aggregationReflections[0].measureFields.length = 0;
      values.aggregationReflections[0].dimensionFields.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      let result = instance.prepare(cloneDeep(values));
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});

      instance.setState({
        mode: 'BASIC'
      });
      result = instance.prepare(cloneDeep(values));
      values.aggregationReflections[0].shouldDelete = true;
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});
    });

    it('should not remove disabled AGGREGATION in BASIC if configured', function() {
      values.aggregationReflections[0].enabled = false;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'BASIC'
      });
      const result = instance.prepare(cloneDeep(values));
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});
    });

    it('should validate raw reflection (missing display)', function() {
      values.rawReflections[0].displayFields.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      expect(instance.prepare(cloneDeep(values))).to.be.eql({errors: {'b': 'At least one display field per raw Reflection is required.'}, reflections: expectedValues});
    });

    it('should validate aggregation reflection (missing measure and dimension)', function() {
      values.aggregationReflections[0].dimensionFields.length = 0;
      values.aggregationReflections[0].measureFields.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      expect(instance.prepare(cloneDeep(values))).to.be.eql({errors: {'a': 'At least one dimension or measure field per aggregation Reflection is required.'}, reflections: expectedValues});
    });

    it('should not validate disabled aggregation reflection', function() {
      values.aggregationReflections[0].enabled = false;
      values.aggregationReflections[0].measureFields.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      expect(instance.prepare(cloneDeep(values))).to.be.eql({errors: {}, reflections: expectedValues});
    });

    it('should filter new reflections that have been deleted', function() {
      values.rawReflections[1].displayFields.shouldDelete = true;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      values.rawReflections.length = 1;
      expect(instance.prepare(cloneDeep(values))).to.be.eql({errors: {}, reflections: [...values.aggregationReflections, ...values.rawReflections]});
    });

    it('all good', function() {
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      const result = instance.prepare(cloneDeep(values));
      expect(result).to.be.eql({errors: {}, reflections: expectedValues});
    });
  });

  describe('#submitForm', function() {
    let props;
    beforeEach(() => {
      props = {
        ...commonProps
      };
    });

    it('should ultimately reject if there is a client-side validation error', function() {
      values.rawReflections[0].displayFields.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should ultimately reject if there is a server-side validation error', function() {
      commonProps.putReflection.rejects();
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it.skip('all good', function() {
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      values.rawReflections.length = 1;
      values.rawReflections[0].shouldDelete = true;
      return instance.submitForm(values).then(data => {
        expect(commonProps.putReflection).to.have.been.calledWith(values.aggregationReflections[0]);
        expect(commonProps.postReflection).to.have.been.calledWith(values.aggregationReflections[1]);
        expect(commonProps.deleteReflection).to.have.been.calledWith(values.rawReflections[0]);
      });
    });
  });

  describe('#renderExtraErrorMessages', () => {
    it('nothing', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      expect(instance.renderExtraErrorMessages().length).to.equal(0);
    });

    it('should render without nav warning if layout found', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'a'}},
        reflections: Immutable.fromJS({a: Immutable.fromJS(values.aggregationReflections[0]).set('status', Immutable.fromJS({}))})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(0);
    });

    it('should render just nav warning as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'foo'}},
        reflections: Immutable.fromJS({a: Immutable.fromJS(values.aggregationReflections[0]).set('status', Immutable.fromJS({}))})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(1);
    });

    it('should render just INVALID warning as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        reflections: Immutable.fromJS({a: Immutable.fromJS(values.aggregationReflections[0]).set('status', Immutable.fromJS({config: 'INVALID'}))})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(1);
    });

    it('should render all messages as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'foo'}},
        reflections: Immutable.fromJS({a: Immutable.fromJS(values.aggregationReflections[0]).set('status', Immutable.fromJS({config: 'INVALID'}))})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(2);
    });
  });
});
