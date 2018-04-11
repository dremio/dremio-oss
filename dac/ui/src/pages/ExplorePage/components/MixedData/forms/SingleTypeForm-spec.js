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

import TransformForm from 'pages/ExplorePage/components/forms/TransformForm';

import { SingleTypeForm } from './SingleTypeForm';

describe('SingleTypeForm', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    minimalProps = {
      fields: {
        desiredType: {
          value: 'TEXT'
        },
        castWhenPossible: {
          value: false
        },
        actionForNonMatchingValue: {
          value: 'DELETE_RECORDS'
        }
      }
    };

    commonProps = {
      ...minimalProps,
      availableValuesCount: 0,
      availableValues: [],
      dataTypes: [{type: 'TEXT'}, {type: 'INTEGER'}]
    };
  });

  describe('render', () => {
    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<SingleTypeForm {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
    });

    it('should render TransformForm, NonMatchingValues and NewFieldSection', () => {
      const wrapper = shallow(<SingleTypeForm {...commonProps}/>);
      expect(wrapper.find(TransformForm)).to.have.length(1);
      expect(wrapper.find('NonMatchingValues')).to.have.length(1);
      expect(wrapper.find('NewFieldSection')).to.have.length(1);
    });
  });

  describe('getCurrentDesiredTypeItem', () => {
    it('should return type based on desiredType and castWhenPossible', () => {
      const singles = ['TEXT', 'INTEGER'].map((desiredType) =>
        [{desiredType, castWhenPossible: true}, {desiredType, castWhenPossible: false}]
      ).reduce((prev, l) => prev.concat(l), []);

      const wrapper = shallow(<SingleTypeForm {...commonProps} singles={singles}/>);
      expect(wrapper.instance().getCurrentDesiredTypeItem()).to.eql({desiredType: 'TEXT', castWhenPossible: false});

      wrapper.setProps({fields: {
        ...commonProps.fields,
        desiredType: {value: 'INTEGER'},
        castWhenPossible: {value: true}
      }});
      expect(wrapper.instance().getCurrentDesiredTypeItem()).to.eql({desiredType: 'INTEGER', castWhenPossible: true});
    });
  });
});
