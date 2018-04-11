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
import Immutable from 'immutable';

import ExtractMapCards from 'pages/ExplorePage/components/Transform/components/forms/sections/ExtractMapCards';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';

import { ExtractMapForm, getExtractMapCards } from './ExtractMapForm';

describe('ExtractMapForm', () => {
  let minimalProps;
  let commonProps;
  let wrapper;
  let instance;
  beforeEach(() => {
    minimalProps = {
      transform: Immutable.Map({
        columnName: 'a'
      }),
      submit: sinon.stub().returns('submitResponse'),
      onCancel: sinon.spy(),
      cards: Immutable.fromJS([{}]),
      fields: { cards: {addField: sinon.spy()}}
    };
    commonProps = {
      ...minimalProps
    };
    wrapper = shallow(<ExtractMapForm {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<ExtractMapForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find(ExtractMapCards)).to.have.length(1);
    expect(wrapper.find(NewFieldSection)).to.have.length(1);
  });

  describe('submit', () => {
    let values;
    beforeEach(() => {
      values = {
        newFieldName: 'a2',
        dropSourceField: false,
        activeCard: 0,
        cards: [{
          type: 'position',
          position: {
            startIndex: { value: 1, direction: 'FROM_THE_START' },
            endIndex: { value: 2, direction: 'FROM_THE_START' }
          }
        }]
      };
    });

    it('should pass submitType to props.submit', () => {
      instance.submit(values, 'apply');
      expect(commonProps.submit.getCall(0).args[1]).to.eql('apply');
    });

    it('should return correct values on submit', () => {
      const expectedResult = {
        ...fieldsMappers.getCommonValues(values, commonProps.transform),
        fieldTransformation: {
          type: 'ExtractMap',
          rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
        }
      };
      expect(instance.submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql(expectedResult);
    });
  });

  describe('getExtractMapCards', () => {
    it('should return empty card if no selection or selection is empty', () => {
      expect(getExtractMapCards()).to.eql(Immutable.fromJS([{type: 'map'}]));
      expect(getExtractMapCards(Immutable.Map())).to.eql(Immutable.fromJS([{type: 'map'}]));
    });

    it('should return card with dotted path', () => {
      expect(
        getExtractMapCards(Immutable.fromJS({mapPathList: ['a', 'b']}))
      ).to.eql(Immutable.fromJS([{type: 'map', path: 'a.b'}]));
    });
  });
});

