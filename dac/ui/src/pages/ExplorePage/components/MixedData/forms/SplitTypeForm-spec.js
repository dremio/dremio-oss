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

import { SplitTypeForm } from './SplitTypeForm';

describe('SplitTypeForm', () => {

  let minimalProps;
  let commonProps;
  beforeEach(() => {
    const selectedTypesList = {value: ['INTEGER']};
    selectedTypesList.onChange = sinon.spy();
    minimalProps = {
      fields: {
        selectedTypesList
      },
      availableValuesCount: 0,
      availableValues: []
    };

    commonProps = {
      ...minimalProps,
      dataTypes: [{type: 'TEXT', matchingPercent: 20}, {type: 'INTEGER', matchingPercent: 50}]
    };
  });

  describe('render', () => {
    it('should render with minimal props without exploding', () => {
      const wrapper = shallow(<SplitTypeForm {...minimalProps}/>);
      expect(wrapper).to.have.length(1);
    });

    it('should render TransformForm, NonMatchingValues and NewFieldSection', () => {
      const wrapper = shallow(<SplitTypeForm {...commonProps}/>);
      expect(wrapper.find(TransformForm)).to.have.length(1);
      expect(wrapper.find('NonMatchingValues')).to.have.length(1);
      expect(wrapper.find('NewFieldSection')).to.have.length(1);
    });

    it('should render one row per data type', () => {
      const wrapper = shallow(<SplitTypeForm {...commonProps}/>);
      expect(wrapper.find('table')).to.have.length(1);
      expect(wrapper.find('tr')).to.have.length(2);
    });

    it('should check selected checkboxes', () => {
      const wrapper = shallow(<SplitTypeForm {...commonProps}/>);
      expect(wrapper.find('Checkbox').at(0).prop('checked')).to.be.false;
      expect(wrapper.find('Checkbox').at(1).prop('checked')).to.be.true;
    });
  });

  describe('toggleType', () => {
    it('should onChange if not already selected', () => {
      const instance = shallow(<SplitTypeForm {...commonProps}/>).instance();
      instance.toggleType('TEXT');
      expect(commonProps.fields.selectedTypesList.onChange).to.have.been.called;
    });

    it('should onChange if selected', () => {
      const instance = shallow(<SplitTypeForm {...commonProps}/>).instance();
      instance.toggleType('INTEGER');
      expect(commonProps.fields.selectedTypesList.onChange).to.have.been.called;
    });
  });
});
