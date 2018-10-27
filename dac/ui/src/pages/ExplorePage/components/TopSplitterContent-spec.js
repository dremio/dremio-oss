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

import { TopSplitterContent } from './TopSplitterContent';

describe('TopSplitterContent', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      sqlState: true,
      sqlSize: 171
    };
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<TopSplitterContent {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('#getHeight', () => {
    it('should return 0 if sqlState is false, or sqlSize = 0', () => {
      const instance = shallow(<TopSplitterContent {...commonProps}/>).instance();
      expect(instance.getHeight(false, 171)).to.eql(0);
      expect(instance.getHeight(true, 0)).to.eql(0);
    });

    it('should return sqlSize if sqlState is true and sqlSize > 0', () => {
      const instance = shallow(<TopSplitterContent {...commonProps}/>).instance();
      expect(instance.getHeight(true, 1)).to.eql(1);
    });
  });
});
