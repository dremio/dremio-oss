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
import Menu from './Menu';

import DividerHr from './DividerHr';

describe('Menu-spec', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {};
    commonProps = {
      ...minimalProps
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<Menu {...minimalProps} />);
    expect(wrapper).to.have.length(1);
  });

  describe('#getItems', () => {
    it('should have falsy items removed', () => {
      const props = {...commonProps, children: [false, 'foo', null]};
      const instance = shallow(<Menu {...props}/>).instance();
      expect(instance.getItems()).to.eql(['foo']);
    });

    it('should have leading divider removed', () => {
      const props = {...commonProps, children: [false, <DividerHr />, 'foo']};
      const instance = shallow(<Menu {...props}/>).instance();
      expect(instance.getItems()).to.eql(['foo']);
    });

    it('should have trailing divider removed', () => {
      const props = {...commonProps, children: ['foo', <DividerHr />, false]};
      const instance = shallow(<Menu {...props}/>).instance();
      expect(instance.getItems()).to.eql(['foo']);
    });

    it('should have double trailing divider removed', () => {
      const props = {...commonProps, children: ['foo', <DividerHr />, <DividerHr />]};
      const instance = shallow(<Menu {...props}/>).instance();
      expect(instance.getItems()).to.eql(['foo']);
    });

    it('should have double divider de-duped', () => {
      const instance = shallow(<Menu
        {...commonProps}
        children={['foo', <DividerHr />, <DividerHr />, 'bar']}
      />).instance();
      const likeInstance = shallow(<Menu
        {...commonProps}
        children={['foo', <DividerHr />, 'bar']}
      />).instance();
      expect(instance.getItems()).to.eql(likeInstance.getItems());
    });
  });
});
