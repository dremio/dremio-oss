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

import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';

import ReplaceFooter from 'pages/ExplorePage/components/Transform/components/ReplaceFooter';
import CustomCondition from 'pages/ExplorePage/components/Transform/components/ContentWithoutCards/CustomCondition';

import { ReplaceCustomForm } from './ReplaceCustomForm';

describe('ReplaceCustomForm', () => {
  let minimalProps;
  let commonProps;
  let values;
  beforeEach(() => {
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'age',
        transformType: 'replace',
        columnType: 'INTEGER'
      }),
      submit: sinon.stub().returns('submitResponse')
    };
    commonProps = {
      ...minimalProps
    };
    values = {
      newFieldName: 'age',
      dropSourceField: false,
      replaceNull:false,
      replacementValue: '',
      replaceType: 'VALUE',
      booleanExpression: 'value IS NOT null \nOR\nvalue IS null'
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplaceCustomForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find(CustomCondition)).to.have.length(1);
  });

  describe('submit', () => {
    it('should pass submitType to props.submit', () => {
      const wrapper = shallow(<ReplaceCustomForm {...commonProps}/>);
      wrapper.instance().submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit when transformType=replace', () => {
      const wrapper = shallow(<ReplaceCustomForm {...commonProps}/>);
      wrapper.instance().submit(values);
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(values, commonProps.transform),
        fieldTransformation: {
          type: 'ReplaceCustom',
          ...fieldsMappers.getReplaceCustom(values)
        }
      });
    });

    it('should return correct values on submit when transformType=exclude', () => {
      const transform = Immutable.Map({
        columnName: 'age',
        transformType: 'exclude',
        columnType: 'INTEGER'
      });
      const wrapper = shallow(<ReplaceCustomForm {...commonProps} transform={transform}/>);
      expect(wrapper.instance().submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludeCustom(values, commonProps.transform)
      });
    });
  });

  describe('renderFooter', function() {
    it('should show replace footer when transformType=replace', () => {
      const wrapper = shallow(<ReplaceCustomForm {...commonProps}/>);
      expect(wrapper.find(ReplaceFooter)).to.have.length(1);
    });
    it('should hide replace footer when transformType!=replace', () => {
      const transform = Immutable.Map({
        transformType: 'exclude'
      });
      const wrapper = shallow(<ReplaceCustomForm {...commonProps} transform={transform}/>);
      expect(wrapper.find(ReplaceFooter)).to.have.length(0);

    });
  });
});

