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
import DragSortColumn from './DragSortColumn';

describe('DragSortColumn', () => {
  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      field: Immutable.Map(),
      dragType: 'default',
      allColumns: Immutable.fromJS([
        {
          name: 'revenue',
          type: 'Integer',
          index: 0
        }
      ])
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<DragSortColumn {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  it('should check that Select, DragAreaColumn, class=drag-sort-column was rendered', () => {
    const wrapper = shallow(<DragSortColumn {...commonProps}/>);
    expect(wrapper.find('Select')).to.length(1);
    expect(wrapper.find('DragAreaColumn')).to.length(1);
    expect(wrapper.hasClass('drag-sort-column')).to.eql(true);
  });
});
