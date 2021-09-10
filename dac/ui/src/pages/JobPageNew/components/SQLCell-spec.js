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
import { mount } from 'enzyme';
import { Tooltip } from '@app/components/Tooltip';
import SQLCell from './SQLCell';

describe('SQLCell', () => {
  let commonProps;
  const context = { loggedInUser: {} };
  beforeEach(() => {
    commonProps = {
      sql: 'SELECT * FROM "nyctaxi-trips-2013.json.gz"'
    };
  });

  it('should render with props without exploding', () => {
    const wrapper = mount(<SQLCell {...commonProps} />, { context });
    expect(wrapper).to.have.length(1);
  });

  it('should show tooltip on mouse hover', () => {
    const wrapper = mount(<SQLCell {...commonProps} />, { context });
    wrapper.simulate('mouseover');
    const component = wrapper.find(Tooltip);
    expect(component.exists()).to.eql(true);
  });
});
