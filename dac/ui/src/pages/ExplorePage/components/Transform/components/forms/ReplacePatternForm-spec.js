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

import { ReplacePatternForm } from './ReplacePatternForm';

describe('ReplacePatternForm', () => {
  let minimalProps;
  let commonProps;
  let values;
  beforeEach(() => {
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'a',
        transformType: 'replace'
      }),
      cards: Immutable.fromJS([{}]),
      fields: { cards: {addField: sinon.spy()}},
      submit: sinon.stub().returns('submitResponse'),
      onCancel: sinon.spy()
    };
    commonProps = {
      ...minimalProps
    };
    values = {
      newFieldName: 'a2',
      dropSourceField: false,
      activeCard: 0,
      replaceSelectionType: 'VALUE',
      replaceTitleType: 'pattern',
      replaceType: 'VALUE',
      replacementValue: 'ss',
      cards: [{
        type: 'replace',
        replace: {
          ignoreCase: false,
          selectionPattern: 'dres',
          selectionType: 'CONTAINS'
        }
      }]
    };
  });

  it('should render with minimal props without exploding', () => {
    const wrapper = shallow(<ReplacePatternForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
  });

  describe('submit', () => {

    it('should pass submitType to props.submit', () => {
      const wrapper = shallow(<ReplacePatternForm {...commonProps}/>);
      wrapper.instance().submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit when transformType=replace', () => {
      const wrapper = shallow(<ReplacePatternForm {...commonProps} />);
      expect(wrapper.instance().submit(values)).to.eql('submitResponse');

      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...fieldsMappers.getCommonValues(values, commonProps.transform),
        fieldTransformation: {
          type: 'ReplacePattern',
          replaceType: fieldsMappers.getReplaceType(values),
          replacementValue: fieldsMappers.getReplacementValue(values),
          rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
        }
      });
    });

    it('should return correct values on submit when transformType=keeponly', () => {
      const transform = Immutable.Map({
        columnName: 'a',
        transformType: 'keeponly'
      });
      const wrapper = shallow(<ReplacePatternForm {...commonProps} transform={transform}/>);
      expect(wrapper.instance().submit(values)).to.eql('submitResponse');

      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql({
        ...filterMappers.getCommonFilterValues(values, transform),
        filter: filterMappers.mapFilterExcludePattern(values, transform)
      });
    });
  });

});
