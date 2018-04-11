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

import {AccelerationAdvanced} from './AccelerationAdvanced';
import AccelerationRaw from './AccelerationRaw';
import AccelerationAggregation from './AccelerationAggregation';

describe('AccelerationAdvanced', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {},
      dataset: Immutable.Map(),
      reflections: Immutable.Map(),
      updateFormDirtyState: sinon.spy(),
      fields: {},
      values: {
        aggregationReflections: [
          {
            fakeFieldList: [],
            booly: true
          }
        ],
        rawReflections: [
          {
            fakeFieldList: [],
            booly: true
          }
        ]
      }
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationAdvanced {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find(AccelerationRaw)).to.have.length(1);
  });

  it('should render AccelerationAggregation if we select tab=Aggregation', () => {
    const wrapper = shallow(<AccelerationAdvanced {...commonProps}/>);
    wrapper.setState({activeTab: 'AGGREGATION'});
    expect(wrapper.find(AccelerationAggregation)).to.have.length(1);
  });

  describe('#getActiveTab()', () => {
    // other cases covered in minimal/common tests
    it('should render AccelerationAggregation as needed if we have such a layoutId', () => {
      const props = {
        ...commonProps,
        location: {state:{layoutId:'foo'}},
        acceleration: Immutable.fromJS({
          aggregationReflections: [{id: {id: 'foo'}}]
        })
      };
      const wrapper = shallow(<AccelerationAdvanced {...props}/>);
      wrapper.setState({activeTab: 'AGGREGATION'});
      expect(wrapper.find(AccelerationAggregation)).to.have.length(1); // use as canary
    });
  });

  describe('#areAdvancedReflectionsFieldsEqual', () => {
    let wrapper;
    let instance;
    let aggregationReflections;
    let rawReflections;
    beforeEach(() => {
      aggregationReflections = Immutable.fromJS(commonProps.values.aggregationReflections);
      rawReflections = Immutable.fromJS(commonProps.values.rawReflections);
      wrapper = shallow(<AccelerationAdvanced {...commonProps}/>);
      instance = wrapper.instance();
    });

    it('should return true when aggregationReflections and rawReflections are equal', () => {
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, rawReflections);

      expect(result).to.be.true;
    });

    it('should return false when aggregationReflections has been changed', () => {
      const changedAggregationReflections = aggregationReflections
        .setIn([0, 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(changedAggregationReflections, rawReflections);

      expect(result).to.be.false;
    });

    it('should return false when rawReflections has been changed', () => {
      const changedRawLayouts = rawReflections
        .setIn([0, 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedRawLayouts);

      expect(result).to.be.false;
    });

    it('should return false if initialReflections don\'t have layoutList from next layout list', () => {
      const changedRawLayouts = Immutable.fromJS([
        { fakeFieldList: [] },
        { anotherFieldList: [] }
      ]);
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedRawLayouts);

      expect(result).to.be.false;
    });

    it('should return true if aggregation field has changed and then has returned to initial state', () => {
      const changedAggregationReflections = aggregationReflections
        .setIn([0, 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(changedAggregationReflections, rawReflections);

      expect(result).to.be.false;

      const initResult = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, rawReflections);

      expect(initResult).to.be.true;
    });

    it('should return true if raw field has changed and then has returned to initial state', () => {
      const changedRawLayouts = rawReflections
        .setIn([0, 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedRawLayouts);

      expect(result).to.be.false;

      const initResult = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, rawReflections);

      expect(initResult).to.be.true;
    });

    it('should return true if we get equal field list but with values in a different order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationReflections: Immutable.fromJS([
            {
              fakeFieldList: [{ name: 'foo' }, { name: 'bar' }],
              booly: true
            }
          ]),
          rawReflections: Immutable.fromJS([
            {
              fakeFieldList: [],
              booly: true
            }
          ])
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationReflections = aggregationReflections
        .setIn([0, 'fakeFieldList'], Immutable.fromJS([ { name: 'bar' }, { name: 'foo' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationReflections, rawReflections);

      expect(result).to.be.true;
    });

    it('should return true if we get sortFields with values in the same order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationReflections: Immutable.fromJS([
            {
              fakeFieldList: [],
              sortFields: [{ name: 'foo' }, { name: 'bar' }],
              booly: true
            }
          ]),
          rawReflections: Immutable.fromJS([
            {
              fakeFieldList: [],
              sortFields: [],
              booly: true

            }
          ])
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationReflections = aggregationReflections
        .setIn([0, 'sortFields'], Immutable.fromJS([ { name: 'foo' }, { name: 'bar' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationReflections, rawReflections);

      expect(result).to.be.true;
    });

    it('should return false if we get sortFields with values in a different order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationReflections: Immutable.fromJS([
            {
              fakeFieldList: [],
              sortFields: [{ name: 'foo' }, { name: 'bar' }],
              booly: true
            }
          ]),
          rawReflections: Immutable.fromJS([
            {
              fakeFieldList: [],
              sortFields: [],
              booly: true
            }
          ])
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationReflections = aggregationReflections
        .setIn([0, 'sortFields'], Immutable.fromJS([ { name: 'bar' }, { name: 'foo' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationReflections, rawReflections);

      expect(result).to.be.false;
    });

    it('should return false if enabled value is different from initial state', () => {
      const changedAggregationReflections = aggregationReflections.setIn([0, 'enabled'], true);
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedAggregationReflections);

      expect(result).to.be.false;

      const changedRawLayouts = rawReflections.setIn([0, 'enabled'], true);
      const anotherResult = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedRawLayouts);

      expect(anotherResult).to.be.false;
    });

    it('should return false if non-array value is different from initial state', () => {
      const changedAggregationReflections = aggregationReflections.setIn([0, 'booly'], false);
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedAggregationReflections);

      expect(result).to.be.false;

      const changedRawLayouts = rawReflections.setIn([0, 'booly'], false);
      const anotherResult = instance.areAdvancedReflectionsFieldsEqual(aggregationReflections, changedRawLayouts);

      expect(anotherResult).to.be.false;
    });
  });

  describe('#componentWillReceiveProps', () => {
    it('should updateFormDirtyState when getting new props', () => {
      const wrapper = shallow(<AccelerationAdvanced {...minimalProps}/>);
      wrapper.setProps(commonProps);

      expect(commonProps.updateFormDirtyState).to.be.called;
    });
  });
});
