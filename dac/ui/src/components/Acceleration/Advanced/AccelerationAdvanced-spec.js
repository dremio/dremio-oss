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

import {AccelerationAdvanced} from './AccelerationAdvanced';
import AccelerationRaw from './AccelerationRaw';
import AccelerationAggregation from './AccelerationAggregation';

describe('AccelerationAdvanced', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      location: {},
      acceleration: Immutable.Map(),
      updateFormDirtyState: sinon.spy(),
      values: {
        aggregationLayouts: {
          enabled: false,
          layoutList: [
            {
              details: {
                fakeFieldList: [],
                booly: true
              }
            }
          ]
        },
        rawLayouts: {
          enabled: false,
          layoutList: [
            {
              details: {
                fakeFieldList: [],
                booly: true
              }
            }
          ]
        }
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
          aggregationLayouts: {layoutList: [{id: {id: 'foo'}}]}
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
    let aggregationLayouts;
    let rawLayouts;
    beforeEach(() => {
      aggregationLayouts = Immutable.fromJS(commonProps.values.aggregationLayouts);
      rawLayouts = Immutable.fromJS(commonProps.values.rawLayouts);
      wrapper = shallow(<AccelerationAdvanced {...commonProps}/>);
      instance = wrapper.instance();
    });

    it('should return true when aggregationLayouts and rawLayouts are equal', () => {
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, rawLayouts);

      expect(result).to.be.true;
    });

    it('should return false when aggregationLayouts has been changed', () => {
      const changedAggregationLayouts = aggregationLayouts
        .setIn(['layoutList', 0, 'details', 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(changedAggregationLayouts, rawLayouts);

      expect(result).to.be.false;
    });

    it('should return false when rawLayouts has been changed', () => {
      const changedRawLayouts = rawLayouts
        .setIn(['layoutList', 0, 'details', 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedRawLayouts);

      expect(result).to.be.false;
    });

    it('should return false if initialLayoutLists don\'t have layoutList from next layout list', () => {
      const changedRawLayouts = rawLayouts.set('layoutList', Immutable.fromJS([
        { details: { fakeFieldList: [] } },
        { details: { anotherFieldList: [] } }
      ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedRawLayouts);

      expect(result).to.be.false;
    });

    it('should return true if aggregation field has changed and then has returned to initial state', () => {
      const changedAggregationLayouts = aggregationLayouts
        .setIn(['layoutList', 0, 'details', 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(changedAggregationLayouts, rawLayouts);

      expect(result).to.be.false;

      const initResult = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, rawLayouts);

      expect(initResult).to.be.true;
    });

    it('should return true if raw field has changed and then has returned to initial state', () => {
      const changedRawLayouts = rawLayouts
        .setIn(['layoutList', 0, 'details', 'fakeFieldList'], Immutable.fromJS([ { name: 'foo' } ]));
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedRawLayouts);

      expect(result).to.be.false;

      const initResult = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, rawLayouts);

      expect(initResult).to.be.true;
    });

    it('should return true if we get equal field list but with values in a different order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [{ name: 'foo' }, { name: 'bar' }],
                  booly: true
                }
              }
            ]
          }),
          rawLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [],
                  booly: true
                }
              }
            ]
          })
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationLayouts = aggregationLayouts
        .setIn(['layoutList', 0, 'details', 'fakeFieldList'], Immutable.fromJS([ { name: 'bar' }, { name: 'foo' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationLayouts, rawLayouts);

      expect(result).to.be.true;
    });

    it('should return true if we get sortFieldList with values in the same order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [],
                  sortFieldList: [{ name: 'foo' }, { name: 'bar' }],
                  booly: true
                }
              }
            ]
          }),
          rawLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [],
                  sortFieldList: [],
                  booly: true
                }
              }
            ]
          })
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationLayouts = aggregationLayouts
        .setIn(['layoutList', 0, 'details', 'sortFieldList'], Immutable.fromJS([ { name: 'foo' }, { name: 'bar' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationLayouts, rawLayouts);

      expect(result).to.be.true;
    });

    it('should return false if we get sortFieldList with values in a different order', () => {
      const props = {
        ...commonProps,
        values: {
          aggregationLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [],
                  sortFieldList: [{ name: 'foo' }, { name: 'bar' }],
                  booly: true
                }
              }
            ]
          }),
          rawLayouts: Immutable.fromJS({
            enabled: false,
            layoutList: [
              {
                details: {
                  fakeFieldList: [],
                  sortFieldList: [],
                  booly: true
                }
              }
            ]
          })
        }
      };
      wrapper = shallow(<AccelerationAdvanced {...props}/>);
      const changedAggregationLayouts = aggregationLayouts
        .setIn(['layoutList', 0, 'details', 'sortFieldList'], Immutable.fromJS([ { name: 'bar' }, { name: 'foo' } ]));
      const result = wrapper.instance().areAdvancedReflectionsFieldsEqual(changedAggregationLayouts, rawLayouts);

      expect(result).to.be.false;
    });

    it('should return false if enabled value is different from initial state', () => {
      const changedAggregationLayouts = aggregationLayouts.set('enabled', true);
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedAggregationLayouts);

      expect(result).to.be.false;

      const changedRawLayouts = rawLayouts.set('enabled', true);
      const anotherResult = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedRawLayouts);

      expect(anotherResult).to.be.false;
    });

    it('should return false if non-array details value is different from initial state', () => {
      const changedAggregationLayouts = aggregationLayouts.setIn(['layoutList', 0, 'details', 'booly'], false);
      const result = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedAggregationLayouts);

      expect(result).to.be.false;

      const changedRawLayouts = rawLayouts.setIn(['layoutList', 0, 'details', 'booly'], false);
      const anotherResult = instance.areAdvancedReflectionsFieldsEqual(aggregationLayouts, changedRawLayouts);

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
