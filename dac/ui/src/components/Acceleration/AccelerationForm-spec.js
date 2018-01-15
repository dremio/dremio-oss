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
import Immutable from 'immutable';

import { ModalForm, FormBody, FormTitle } from 'components/Forms';
import Button from '../Buttons/Button';
import { AccelerationForm } from './AccelerationForm';

describe('AccelerationForm', () => {
  let minimalProps;
  let commonProps;
  let values;

  beforeEach(() => {
    minimalProps = {
      handleSubmit: sinon.stub().returns(sinon.spy()),
      onCancel: sinon.spy(),
      location: {}
    };
    commonProps = {
      showConfirmationDialog: sinon.spy(),
      acceleration: Immutable.Map(),
      fullPath: '',
      submit: sinon.stub().returns(Promise.resolve()),
      location: {},
      fields: {},
      errors: {},
      submitFailed: false,
      ...minimalProps
    };

    values = {
      version: 1,
      aggregationLayouts: {
        // WARNING: this might not be exactly accurate - but it's enough for the test
        layoutList: [
          {id:'a', details: {measureFieldList: ['cm1'], dimensionFieldList: ['cd1']}},
          {id:null, details: {measureFieldList: ['cm1'], dimensionFieldList: ['cd1']}}
        ],
        enabled: true
      },
      rawLayouts: {
        // WARNING: this might not be exactly accurate - but it's enough for the test
        layoutList: [
          {id:'b', details: {displayFieldList: ['cm1']}},
          {id:null, details: {displayFieldList: ['cm1']}}
        ],
        enabled: true
      },
      columnsDimensions: [{column: 'cd1'}],
      columnsMeasures: [{column: 'cm1'}]
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

  describe('#onSwitchToAdvanced', function() {
    it('should call showConfirmationDialog if dirty is true', function() {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      wrapper.setProps({
        dirty: true
      });
      const instance = wrapper.instance();

      instance.onSwitchToAdvanced();
      expect(commonProps.showConfirmationDialog).to.be.called;
    });
    it('should call toggleMode if dirty is false', function() {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      sinon.spy(instance, 'toggleMode');

      instance.onSwitchToAdvanced();
      expect(instance.toggleMode).to.be.called;
    });
  });

  describe('#toggleMode', function() {
    it('should toggle mode value when value valid', function() {
      const instance = shallow(<AccelerationForm {...commonProps}/>).instance();
      instance.setState({
        mode: 'AUTO'
      });
      instance.toggleMode();
      expect(instance.state.mode).to.be.equal('MANUAL');
    });

    it('should not toggle mode value when value invalid', function() {
      const instance = shallow(<AccelerationForm {...commonProps}/>).instance();
      instance.setState({
        mode: null
      });
      instance.toggleMode();
      expect(instance.state.mode).to.be.null;
    });
  });

  describe('#canSubmit', function() {
    it('should return true when acceleartion unknown', function() {
      const instance = shallow(<AccelerationForm {...commonProps}/>).instance();
      expect(instance.canSubmit()).to.be.true;
    });

    it('should return false when acceleartion has state NEW', function() {
      const props = {...commonProps, acceleration: Immutable.Map({state: 'NEW'})};
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      expect(instance.canSubmit()).to.be.false;
    });
  });

  describe('#submitForm', function() {
    let props;
    beforeEach(() => {
      props = {
        ...commonProps,
        acceleration: Immutable.fromJS({
          state: 'NEW',
          mode: 'AUTO',
          id: {},
          type: 't1',
          version: 1,
          context: {
            dataset: Immutable.Map(),
            datasetSchema: Immutable.Map()
          }
        })
      };
    });


    it('should validate layouts in MANUAL with rawLayouts enabled (missing display)', function() {
      values.rawLayouts.layoutList[0].details.displayFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should not validate layouts in MANUAL with rawLayouts disabled', function() {
      values.rawLayouts.enabled = false;
      values.rawLayouts.layoutList[0].details.displayFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });

    it('should not validate layouts in AUTO', function() {
      values.rawLayouts.layoutList[0].details.displayFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });


    it('should validate layouts in AUTO with aggregationLayouts enabled (missing measure)', function() {
      values.columnsMeasures.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should validate layouts in AUTO with aggregationLayouts enabled (missing dimension)', function() {
      values.columnsDimensions.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should not validate layouts in AUTO with aggregationLayouts disabled', function() {
      values.aggregationLayouts.enabled = false;
      values.columnsDimensions.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });

    it('should not validate MANUAL layouts in AUTO', function() {
      values.aggregationLayouts.layoutList[0].details.measureFieldList.length = 0; // invalid for MANUAL
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });

    it('should validate layouts in MANUAL with aggregationLayouts enabled (missing measure)', function() {
      values.aggregationLayouts.layoutList[0].details.measureFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should validate layouts in MANUAL with aggregationLayouts enabled (missing dimension)', function() {
      values.aggregationLayouts.layoutList[0].details.dimensionFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.be.rejected;
    });

    it('should not validate layouts in MANUAL with aggregationLayouts disabled', function() {
      values.aggregationLayouts.enabled = false;
      values.aggregationLayouts.layoutList[0].details.measureFieldList.length = 0;
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });

    it('should not validate AUTO layouts in MANUAL', function() {
      values.columnsDimensions.length = 0; // invalid for AUTO
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.setState({
        mode: 'MANUAL'
      });
      return expect(instance.submitForm(values)).to.not.be.rejected;
    });

    it('submit should be called if all good', function() {
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      instance.submitForm(values);
      expect(commonProps.submit.getCall(0).args[0]).to.be.eql({
        'aggregationLayouts': {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [
            {id:'a', details: {measureFieldList: ['cm1'], dimensionFieldList: ['cd1']}},
            {details: {measureFieldList: ['cm1'], dimensionFieldList: ['cd1']}}
          ],
          enabled: true
        },
        'rawLayouts': {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [
            {id:'b', details: {displayFieldList: ['cm1']}},
            {details: {displayFieldList: ['cm1']}}
          ],
          enabled: true
        },
        'context': {
          'dataset': {},
          'datasetSchema': {},
          'logicalAggregation': {
            'dimensionList': [
              {
                'name': 'cd1'
              }
            ],
            'measureList': [
              {
                'name': 'cm1'
              }
            ]
          }
        },
        'id': {},
        'mode': 'AUTO',
        'version': 1,
        'state': 'NEW',
        'type': 't1'
      });
    });
  });

  describe('#renderHeader', function() { // todo: these tests way too brittle

    it('should render FormTitle with Button with common props', function() {
      const props = {...commonProps, acceleration: Immutable.Map({mode: 'AUTO'})};
      const instance = shallow(<AccelerationForm {...props}/>).instance();
      expect(instance.renderHeader()).to.be.eql(
        <div>
          <div style={{float: 'right'}}>
            <Button
              disableSubmit
              onClick={instance.clearReflections}
              type='CUSTOM' text={la('Clear All Reflections')}
            />
            <Button
              disableSubmit onClick={instance.onSwitchToAdvanced}
              style={{ marginLeft: 10 }}
              type='CUSTOM'
              text={la('Switch to Advanced')}
            />
          </div>
          <FormTitle>
            {la('Reflections')}
          </FormTitle>
        </div>
      );
    });

    it('should render FormTitle without Button with minimal props', function() {
      const instance = shallow(<AccelerationForm {...minimalProps}/>).instance();
      expect(instance.renderHeader()).to.be.eql(
        <div>
          <div style={{float: 'right'}}>
            <Button
              disableSubmit
              onClick={instance.clearReflections}
              type='CUSTOM' text={la('Clear All Reflections')}
            />
            {null}
          </div>
          <FormTitle>
            {la('Reflections')}
          </FormTitle>
        </div>
      );
    });
  });

  describe('#renderExtraErrorMessages', () => {
    it('should render without nav warning and errorList of there are none', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      expect(instance.renderExtraErrorMessages().length).to.equal(0);
    });

    it('should render without nav warning if layout found', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'a'}},
        acceleration: commonProps.acceleration.merge(values)
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(0);
    });

    it('should render just errorList as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        acceleration: commonProps.acceleration.merge({errorList: ['error']})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(1);
    });

    it('should render just nav warning as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'foo'}},
        acceleration: commonProps.acceleration.merge(values)
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(1);
    });

    it('should render just OUT_OF_DATE warning as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        acceleration: commonProps.acceleration.merge({state: 'OUT_OF_DATE'})
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(1);
    });

    it('should render all messages as needed', () => {
      const wrapper = shallow(<AccelerationForm {...commonProps}/>);
      const instance = wrapper.instance();
      wrapper.setProps({
        location: {state: { layoutId: 'foo'}},
        acceleration: commonProps.acceleration.merge({state: 'OUT_OF_DATE', errorList: ['error']}).merge(values)
      });
      expect(instance.renderExtraErrorMessages().length).to.equal(3);
    });
  });

  describe('#deleteAcceleration', () => {
    it('should call props.onCancel when acceleration was deleted', () => {
      const props = {
        ...commonProps,
        deleteAcceleration: sinon.stub().returns(Promise.resolve()),
        updateFormDirtyState: sinon.stub(),
        acceleration: Immutable.Map({ id: { id: 'abc' } })
      };
      const instance = shallow(<AccelerationForm {...props}/>).instance();

      instance.deleteAcceleration().then(() => {
        expect(props.onCancel).to.have.been.called;
      });
      expect(props.updateFormDirtyState).to.be.calledWith(false);
    });
  });
});
