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

import DragColumnMenu from 'components/DragComponents/DragColumnMenu';
import AggregateContent from './AggregateContent';

describe('AggregateContent', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      fields: {
        columnsDimensions: [],
        columnsMeasures: []
      },
      dragType: 'dimension'
    };

    minimalProps.fields.columnsDimensions.addField = sinon.spy();
    minimalProps.fields.columnsDimensions.removeField = sinon.spy();
    minimalProps.fields.columnsMeasures.addField = sinon.spy();
    minimalProps.fields.columnsMeasures.removeField = sinon.spy();

    commonProps = {
      ...minimalProps,
      allColumns: Immutable.List()
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AggregateContent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should renders <div>, DragColumnMenu, DimensionDragArea', () => {
    const wrapper = shallow(<AggregateContent {...commonProps}/>);
    expect(wrapper.type()).to.equal('div');
    expect(wrapper.find('.aggregate-content')).to.have.length(1);
    expect(wrapper.find('DragColumnMenu')).to.have.length(1);
    expect(wrapper.find('ColumnDragArea')).to.have.length(1);
    expect(wrapper.find('MeasureDragArea')).to.have.length(1);
  });

  it('should render DragColumnMenu with available columns', () => {
    const props = {
      dragType: 'dimension',
      allColumns: Immutable.fromJS([
        {name: 'col1'},
        {name: 'col2'},
        {name: 'col3'}
      ]),
      canSelectMeasure: false,
      canUseFieldAsBothDimensionAndMeasure: true,
      fields: {
        columnsDimensions: [
          {column: {value: 'col1'}},
          {column: {value: 'col2'}}
        ],
        columnsMeasures: [
          {column: {value: 'col1'}},
          {column: {value: 'col3'}}
        ]
      }
    };
    const wrapper = shallow(<AggregateContent {...props}/>);
    expect(wrapper.find(DragColumnMenu).props().disabledColumnNames).to.be.eql(Immutable.Set(['col1']));

    // add new dimension column that also selected for measures
    // Must be new object to override caching
    props.fields = {...props.fields, columnsDimensions: [...props.fields.columnsDimensions, {column: {value: 'col3'}}]};
    wrapper.setProps(props);
    // check for available column updates
    expect(wrapper.find(DragColumnMenu).props().disabledColumnNames).to.be.eql(Immutable.Set(['col1', 'col3']));
  });

  describe('#receiveProps', () => {
    it(`should only call getDisabledColumns if one of allColumns, fields, 
canSelectMeasure or canUseFieldAsBothDimensionAndMeasure has changed`, () => {
      const wrapper = shallow(<AggregateContent {...commonProps}/>);
      const instance = wrapper.instance();
      sinon.spy(instance, 'getDisabledColumnNames');
      instance.receiveProps(commonProps, commonProps);
      expect(instance.getDisabledColumnNames).to.not.be.called;

      instance.receiveProps({...commonProps, dragType: 'foo'}, commonProps);
      expect(instance.getDisabledColumnNames).to.not.be.called;

      instance.receiveProps({...commonProps, canSelectMeasure: false}, commonProps);
      expect(instance.getDisabledColumnNames).to.have.callCount(1);

      instance.receiveProps({...commonProps, canUseFieldAsBothDimensionAndMeasure: false}, commonProps);
      expect(instance.getDisabledColumnNames).to.have.callCount(2);

      instance.receiveProps({...commonProps, allColumns: Immutable.fromJS([{name: 'foo'}])}, commonProps);
      expect(instance.getDisabledColumnNames).to.have.callCount(3);

      instance.receiveProps({...commonProps, fields: {columnsDimensions: [], columnsMeasures: []}}, commonProps);
      expect(instance.getDisabledColumnNames).to.have.callCount(4);
    });
  });

  describe('#getDisabledColumnNames', () => {
    let wrapper;
    let instance;
    beforeEach(() => {
      wrapper = shallow(<AggregateContent {...commonProps}/>);
      instance = wrapper.instance();
    });

    it('should return columns that not present in both dimension and ' +
      'measures lists if canSelectMeasure is false', () => {
      const props = {
        allColumns: Immutable.fromJS([
          {name: 'col1'},
          {name: 'col2'},
          {name: 'col3'},
          {name: 'col4'}
        ]),
        canSelectMeasure: false,
        canUseFieldAsBothDimensionAndMeasure: true,
        fields: {
          columnsDimensions: [
            {column: {value: 'col1'}},
            {column: {value: 'col2'}}
          ],
          columnsMeasures: [
            {column: {value: 'col1'}},
            {column: {value: 'col3'}}
          ]
        }
      };
      wrapper.setProps(props);
      const expectedColumnNames = Immutable.Set(['col1']);
      expect(instance.getDisabledColumnNames(props)).to.be.eql(expectedColumnNames);
    });

    it('should return columns that are absent in both dimension and measures lists ' +
       'when canUseFieldAsBothDimensionAndMeasure is false from props', () => {
      const props = {
        allColumns: Immutable.fromJS([
          {name: 'col1'},
          {name: 'col2'},
          {name: 'col3'},
          {name: 'col4'}
        ]),
        canSelectMeasure: true,
        canUseFieldAsBothDimensionAndMeasure: false,
        fields: {
          columnsDimensions: [
            {column: {value: 'col2'}}
          ],
          columnsMeasures: [
            {column: {value: 'col1'}},
            {column: {value: 'col3'}}
          ]
        }
      };
      wrapper.setProps(props);
      const expectedColumnNames = Immutable.Set(['col1', 'col2', 'col3']);
      expect(instance.getDisabledColumnNames(props)).to.be.eql(expectedColumnNames);
    });
  });
});
