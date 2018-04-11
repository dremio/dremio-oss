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

import {
  TransformHeader,
  EXTRACT_TAB,
  SPLIT_TAB,
  REPLACE_TAB,
  KEEP_ONLY_TAB,
  EXCLUDE_TAB
} from './TransformHeader';

describe('TransformHeader', () => {

  let commonProps;
  let context;
  beforeEach(() => {
    commonProps = {
      transform: Immutable.Map({
        column: 'foo'
      }),
      closeIconHandler: sinon.spy(),
      closeIcon: true,
      separator: ' ',
      text: 'text',
      location: {
        query: {
          hasSelection: true
        },
        state: {
          listOfItems: '',
          columnType: 'TEXT'
        }
      }
    };
    context = {
      router : {push: sinon.spy()}
    };
  });

  it('should render .raw-wizard-header', () => {
    const wrapper = shallow(<TransformHeader {...commonProps}/>, {context});
    const children = wrapper.children();
    const content = children.at(0);

    const {location, transform} = commonProps;
    expect(content.hasClass('raw-wizard-header')).to.equal(true);
    expect(content.find('Link').length).to.equal(3);
    expect(content.find('Link').at(0).props().to).to.eql(
      {...location, state: {...transform.toJS(), transformType: 'replace'}}
    );
  });

  describe('#isTabEnabled', () => {
    it('should return true if id=extract and type of column is LIST or MAP', () => {
      const props = {
        ...commonProps,
        transform: Immutable.Map({
          column: 'foo',
          columnType: 'LIST'
        })
      };
      const wrapper = shallow(<TransformHeader {...props}/>, {context});
      const instance = wrapper.instance();

      expect(instance.isTabEnabled(EXTRACT_TAB)).to.eql(true);

      wrapper.setProps({ transform: Immutable.Map({ column: 'foo', columnType: 'MAP'}) });

      expect(instance.isTabEnabled(EXTRACT_TAB)).to.eql(true);
      expect(instance.isTabEnabled(REPLACE_TAB)).to.eql(false);
      expect(instance.isTabEnabled(KEEP_ONLY_TAB)).to.eql(false);
      expect(instance.isTabEnabled(SPLIT_TAB)).to.eql(false);
      expect(instance.isTabEnabled(EXCLUDE_TAB)).to.eql(false);
    });

    it('should return true if id=replace/keeponly/exclude and type of column is not TEXT type', () => {
      const props = {
        ...commonProps,
        transform: Immutable.Map({
          column: 'foo',
          columnType: 'INTEGER'
        })
      };

      const wrapper = shallow(<TransformHeader {...props}/>, {context});
      const instance = wrapper.instance();

      expect(instance.isTabEnabled(SPLIT_TAB)).to.eql(false);
      expect(instance.isTabEnabled(EXTRACT_TAB)).to.eql(false);
      expect(instance.isTabEnabled(REPLACE_TAB)).to.eql(true);
      expect(instance.isTabEnabled(KEEP_ONLY_TAB)).to.eql(true);
      expect(instance.isTabEnabled(EXCLUDE_TAB)).to.eql(true);
    });

    it('should return true for all tabs if type of column is TEXT', () => {
      const props = {
        ...commonProps,
        transform: Immutable.Map({
          column: 'foo',
          columnType: 'TEXT'
        })
      };

      const wrapper = shallow(<TransformHeader {...props}/>, {context});
      const instance = wrapper.instance();

      expect(instance.isTabEnabled(SPLIT_TAB)).to.eql(true);
      expect(instance.isTabEnabled(EXTRACT_TAB)).to.eql(true);
      expect(instance.isTabEnabled(REPLACE_TAB)).to.eql(true);
      expect(instance.isTabEnabled(KEEP_ONLY_TAB)).to.eql(true);
      expect(instance.isTabEnabled(EXCLUDE_TAB)).to.eql(true);
    });
  });
});
