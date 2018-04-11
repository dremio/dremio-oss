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

import {AccelerationBasic} from './AccelerationBasic';

const context = {
  context: {
    reflectionSaveErrors: Immutable.fromJS({})
  }
};

describe('AccelerationBasic', () => {
  let minimalProps;
  let commonProps;

  beforeEach(() => {
    minimalProps = {
      location: {},
      dataset: Immutable.fromJS({
        id: '1',
        path: ['path', 'name']
      }),
      reflections: Immutable.fromJS({
        a: {id: 'a', type: 'AGGREGATION', measureFields: ['cm1'], dimensionFields: ['cd1']},
        b: {id: 'b', type: 'RAW', displayFields: ['cm1']}
      }),
      fields: {
        rawReflections: [],
        aggregationReflections: []
      }
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<AccelerationBasic {...minimalProps}/>, context);
    expect(wrapper).to.have.length(1);
  });

  it('should render with common props without exploding', () => {
    const wrapper = shallow(<AccelerationBasic {...commonProps}/>, context);
    expect(wrapper).to.have.length(1);
  });

  describe('#getHighlightedSection()', () => {
    it('none', () => {
      const instance = shallow(<AccelerationBasic {...commonProps}/>, context).instance();
      expect(instance.getHighlightedSection()).to.be.equal(null);
    });

    it('AGGREGATION', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'a'}}}/>, context).instance();
      expect(instance.getHighlightedSection()).to.be.equal('AGGREGATION');
    });

    it('RAW', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'b'}}}/>, context).instance();
      expect(instance.getHighlightedSection()).to.be.equal('RAW');
    });

    it('missing', () => {
      const instance = shallow(<AccelerationBasic {...commonProps} location={{state:{layoutId:'n/a'}}}/>, context).instance();
      expect(instance.getHighlightedSection()).to.be.equal(undefined);
    });
  });
});
