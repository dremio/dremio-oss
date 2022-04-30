/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { render } from 'rtlUtils';
import Immutable from 'immutable';
import FilterSelectMenu from './FilterSelectMenu';

describe('FilterSelectMenu', () => {

  let commonProps;
  beforeEach(() => {
    commonProps = {
      name: 'name',
      label: 'label',
      items: [
        {label: 'item3', id: 3},
        {label: 'item2', id: 2},
        {label: 'item1', id: 1}
      ],
      selectedValues: Immutable.List([2, 3]),
      onItemSelect: sinon.spy(),
      onItemUnselect: sinon.spy()
    };
  });

  it('should render with minimal props without exploding', () => {
    const { queryByTestId } = render(<FilterSelectMenu {...commonProps}/>);
    expect(queryByTestId('name-filter')).to.be.ok;
  });
});
