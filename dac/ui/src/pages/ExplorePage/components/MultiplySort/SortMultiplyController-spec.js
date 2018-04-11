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
import SortMultiplyController from './SortMultiplyController';

describe('SortMultiplyController', () => {

  let minimalProps;
  let commonProps;
  let wrapper;
  beforeEach(() => {
    minimalProps = {
      location: {
        state: {}
      },
      fields: {},
      columns: Immutable.List()
    };
    commonProps = {
      ...minimalProps,
      columns: Immutable.fromJS([
        {
          name: 'revenue',
          type: 'Integer',
          index: 0
        },
        {
          name: 'age',
          type: 'Text',
          index: 1
        }
      ])
    };
    wrapper = shallow(<SortMultiplyController {...commonProps}/>);
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<SortMultiplyController {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });
});
