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

import AccelerationAggregation from './AccelerationAggregation';

describe('AccelerationAggregation', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      dataset: Immutable.fromJS({
        id: '1',
        path: ['path', 'name']
      }),
      reflections: Immutable.fromJS({}),
      fields: {
        aggregationReflections: [{name:{value:'col1'}}, {name:{value:'col2'}}]
      }
    };
    minimalProps.fields.aggregationReflections.addField = sinon.spy();
    minimalProps.fields.aggregationReflections.removeField = sinon.spy();
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<AccelerationAggregation {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    const wrap = shallow(<AccelerationAggregation {...minimalProps}/>);
    expect(wrap).to.have.length(1);
  });

  it.skip('should add new layout', () => {
    const defaultLayout = {
      enabled: true,
      distributionFields: [],
      partitionFields: [],
      sortFields: [],
      measureFields: [],
      dimensionFields: [],
      partitionDistributionStrategy: 'CONSOLIDATED',
      shouldDelete: false,
      name: 'New Reflection',
      type: 'AGGREGATION'
    };
    instance.addNewLayout();
    expect(commonProps.fields.aggregationReflections.addField).to.have.been.calledWithMatch(defaultLayout);
  });
});
