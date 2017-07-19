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

import {AccelerationBasic} from './AccelerationBasic';

describe('AccelerationBasic', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {
      location: {},
      acceleration: Immutable.fromJS({
        version: 1,
        aggregationLayouts: {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [{id:{id:'a'}, details: {measureFieldList: ['cm1'], dimensionFieldList: ['cd1']}}],
          enabled: true
        },
        rawLayouts: {
          // WARNING: this might not be exactly accurate - but it's enough for the test
          layoutList: [{id:{id:'b'}, details: {displayFieldList: ['cm1']}}],
          enabled: true
        },
        columnsDimensions: [{column: 'cd1'}],
        columnsMeasures: [{column: 'cm1'}]
      })
    };
    commonProps = {
      ...minimalProps,
      fullPath: '',
      fields: {
        rawLayouts: {
          enabled: true
        }
      }
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationBasic {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<AccelerationBasic {...commonProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#getHighlightedSection()', () => {
    it('none', () => {
      const instance = shallow(<AccelerationBasic {...commonProps}/>).instance();
      expect(instance.getHighlightedSection()).to.be.equal(null);
    });

    it('AGGREGATION', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'a'}}}/>).instance();
      expect(instance.getHighlightedSection()).to.be.equal('AGGREGATION');
    });

    it('RAW', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'b'}}}/>).instance();
      expect(instance.getHighlightedSection()).to.be.equal('RAW');
    });

    it('missing', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'n/a'}}}/>).instance();
      expect(instance.getHighlightedSection()).to.be.equal(null);
    });
  });
});
