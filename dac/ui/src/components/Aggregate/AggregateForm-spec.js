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

import { stubArrayFieldMethods } from 'testUtil';
import AggregateContent from './AggregateContent';
import AggregateFooter from './AggregateFooter';

import AggregateForm from './AggregateForm';

describe('AggregateForm', () => {

  let commonProps;
  let minimalProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.Map(),
      columns: Immutable.List(),
      fields: {},
      submit: sinon.spy(),
      onCancel: sinon.spy(),
      changeFormType: sinon.spy(),
      location: {state: {}},
      handleSubmit: sinon.spy()
    };
    commonProps = {
      ...minimalProps,
      fields: {
        columnsDimensions: stubArrayFieldMethods([
          { column: { value: 'col2' } }
        ]),
        columnsMeasures: stubArrayFieldMethods({
          find: sinon.spy(),
          length: 0
        })
      },
      columns: Immutable.fromJS([{
        name: 'col1',
        type: 'TEXT'
      }])
    };

    commonProps.fields.columnsDimensions.addField = sinon.stub().resolves();
    commonProps.fields.columnsDimensions.removeField = sinon.stub().resolves();

    wrapper = shallow(<AggregateForm {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<AggregateForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render <div>, AggregateContent, AggregateFooter', () => {
    expect(wrapper.type()).to.equal('div');
    expect(wrapper.find('.aggregate-form')).to.have.length(1);
    expect(wrapper.find(AggregateContent)).to.have.length(1);
    expect(wrapper.find(AggregateFooter)).to.have.length(1);
  });

  describe('#onDragStart', () => {
    it('should set isDragInProgress=true', () => {
      instance.onDragStart();
      expect(instance.state.isDragInProgress).to.eql(true);
    });
  });

  describe('#handleDrop', () => {
    it('should set isDragInProgress=false', () => {
      instance.handleDrop('measures', {id: 'col1'});
      expect(instance.state.isDragInProgress).to.eql(false);
    });

    it('should move column from dimensions to measures', () => {
      const dropData = {id: 'col1', index: 0, type: 'dimensions'};
      instance.handleDrop('measures', dropData);
      expect(commonProps.fields.columnsDimensions.removeField).to.have.been.calledWith(dropData.index);
      expect(commonProps.fields.columnsMeasures.addField).to.have.been.calledWith({
        column: 'col1',
        measure: 'Count'
      });
    });

    it('should move column from measures to dimensions', () => {
      const dropData = {id: 'col1', index: 0, type: 'measures'};
      instance.handleDrop('dimensions', dropData);
      expect(commonProps.fields.columnsMeasures.removeField).to.have.been.calledWith(dropData.index);
      expect(commonProps.fields.columnsDimensions.addField).to.have.been.calledWith({
        column: 'col1'
      });
    });

    it('should add column to measures when dragColumnType is measures', () => {
      instance.handleDrop('measures', {id: 'col1'});
      expect(commonProps.fields.columnsMeasures.addField).to.have.been.calledWith({
        column: 'col1',
        measure: 'Count'
      });
    });

    it('should add column to dimensions when dragOrigin is dimensions', () => {
      instance.handleDrop('dimensions', {id: 'col1'});
      expect(commonProps.fields.columnsDimensions.addField).to.have.been.calledWith({
        column: 'col1'
      });
    });

    it('should not add column to dimensions when dragOrigin is dimensions if it is already selected', () => {
      instance.handleDrop('dimensions', {id: 'col2'});
      expect(commonProps.fields.columnsDimensions.addField.called).to.be.false;
    });

    it('should not add column if dragOrigin of drag area is the same', () => {
      instance.handleDrop('dimensions', {id: 'col2', type: 'dimensions'});
      expect(commonProps.fields.columnsDimensions.addField.called).to.be.false;
    });
  });

  describe('stopDrag', () => {
    it('should set isDragInProgress=false', () => {
      instance.stopDrag();
      expect(instance.state.isDragInProgress).to.eql(false);
    });
  });

  describe('addAnother', () => {
    it('should add another column used list of columns if type is measures', () => {
      instance.addAnother('measures');
      expect(commonProps.fields.columnsMeasures.addField).to.have.been.calledWith({measure: 'Sum'});
    });

    it('should add another column used list of columns if type is not measures', () => {
      instance.addAnother('');
      expect(commonProps.fields.columnsDimensions.addField).to.have.been.calledWith({});
    });
  });

  describe('#handleClearAllDimensions', () => {
    it('should clear all dimensions column', () => {
      const calledTimes = commonProps.fields.columnsDimensions.length;
      instance.handleClearAllDimensions();
      expect(commonProps.fields.columnsDimensions.removeField).to.have.callCount(calledTimes);
    });
  });

  describe('#handleClearAllMeasures', () => {
    let fields;
    beforeEach(() => {
      fields = {
        columnsMeasures: stubArrayFieldMethods([ {}, {}, {} ])
      };
      wrapper.setProps({ fields });
    });

    it('should clear all measures column', (done) => {
      const calledTimes = fields.columnsMeasures.length;

      fields.columnsMeasures.removeField = sinon.stub().resolves();

      instance.handleClearAllMeasures();

      setTimeout(() => {
        expect(fields.columnsMeasures.removeField).to.have.callCount(calledTimes);
        done();
      }, 100);
    });
  });
});
