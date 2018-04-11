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

import { Toggle } from 'components/Fields';
import AggregateForm from 'components/Aggregate/AggregateForm';
import AccelerationAggregate from './AccelerationAggregate';

describe('AccelerationAggregate', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({
        fields: [
          {name: 'c1', type: {name: 't1'}}
        ]
      }),
      fields: {
        aggregationReflections: [{
          enabled: true
        }]
      }
    };
    commonProps = {
      location: {},
      style: {},
      textStyle: {},
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationAggregate {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<AccelerationAggregate {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render Toggle with common props', () => {
    const wrapper = shallow(<AccelerationAggregate {...commonProps}/>);
    expect(wrapper.find(Toggle)).to.have.length(1);
  });

  it('should render AggregateForm with common props', () => {
    const wrapper = shallow(<AccelerationAggregate {...commonProps}/>);
    expect(wrapper.find(AggregateForm)).to.have.length(1);
  });

  describe('#getFields', function() {

    it('AggregateForm.getFields should be called', function() {
      sinon.spy(AggregateForm, 'getFields');
      AccelerationAggregate.getFields();
      expect(AggregateForm.getFields).to.be.calledOnce;
    });
  });

  describe('#validate', function() {

    it('AggregateForm.validate should be called', function() {
      sinon.spy(AggregateForm, 'validate');
      AccelerationAggregate.validate([]);
      expect(AggregateForm.validate).to.be.calledWith([]);
    });
  });

  describe('#mapSchemaToColumns', function() {

    it('should return columns', function() {
      const instance = shallow(<AccelerationAggregate {...commonProps}/>).instance();
      const acceleration = Immutable.fromJS({ // update
        context: {
          datasetSchema: {
            fieldList: [
              {
                type: 't1',
                name: 'c1'
              }
            ]
          }
        }
      });
      expect(instance.mapSchemaToColumns(acceleration)).to.be.eql(Immutable.fromJS([
        {
          type: 't1',
          name: 'c1',
          index: 0
        }
      ]));
    });
  });
});
