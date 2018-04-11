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

import { BOOLEAN, TEXT, DATE } from 'constants/DataTypes';

import ReplaceFooter from './ReplaceFooter';

describe('ReplaceFooter', () => {
  let commonProps;
  let booleanProps;
  let dateProps;
  beforeEach(() => {
    commonProps = {
      transform: Immutable.fromJS({
        method: '',
        columnName: 'text',
        columnType: TEXT
      }),
      fields: {
        replaceType: '',
        replacementValue: '',
        replaceSelectionType: ''
      }
    };
    booleanProps = {
      ...commonProps,
      transform: Immutable.fromJS({
        method: '',
        columnName: 'bool',
        columnType: BOOLEAN
      })
    };
    dateProps = {
      ...commonProps,
      transform: Immutable.fromJS({
        method: '',
        columnName: 'date',
        columnType: DATE
      })
    };
  });

  it('renders', () => {
    const wrapper = shallow(<ReplaceFooter {...commonProps} />);
    expect(wrapper.type()).to.eql('div');
  });

  describe('renderReplaceValueInput', () => {
    it('should render BooleanSelect if columnType is BOOLEAN', () => {
      const wrapper = shallow(<ReplaceFooter {...booleanProps} />);
      expect(wrapper.find('BooleanSelect')).to.have.length(1);
    });
    it('should render DateInput if columnType is dateType', () => {
      const wrapper = shallow(<ReplaceFooter {...dateProps} />);
      expect(wrapper.find('DateInput')).to.have.length(1);
    });
    it('should render text input', () => {
      const wrapper = shallow(<ReplaceFooter {...commonProps} />);
      expect(wrapper.find('BooleanSelect')).to.have.length(0);
      expect(wrapper.find('DateInput')).to.have.length(0);
      expect(wrapper.find('TextField')).to.have.length(1);
    });
  });
});
