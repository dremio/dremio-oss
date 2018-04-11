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

import ExtractListCards from 'pages/ExplorePage/components/Transform/components/forms/sections/ExtractListCards';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';

import { ExtractListForm, getListTransformCards } from './ExtractListForm';

describe('ExtractListForm', () => {
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
    wrapper = shallow(<ExtractListForm {...commonProps}/>);
    instance = wrapper.instance();
  });

  it('should render with minimal props without exploding', () => {
    wrapper = shallow(<ExtractListForm {...minimalProps}/>);
    expect(wrapper).to.have.length(1);
    expect(wrapper.find(ExtractListCards)).to.have.length(1);
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
          type:'ExtractList',
          rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
        }
      };
      expect(instance.submit(values)).to.eql('submitResponse');
      expect(commonProps.submit.calledOnce).to.eql(true);
      expect(commonProps.submit.getCall(0).args[0]).to.eql(expectedResult);
    });
  });

  describe('getListTransformCards', () => {
    it('should return empty single card if no selection or selection is empty', () => {
      expect(getListTransformCards()).to.eql(Immutable.fromJS([{type: 'single'}]));
      expect(getListTransformCards(Immutable.Map())).to.eql(Immutable.fromJS([{type: 'single'}]));
    });

    it('should return single index card if only one index selected', () => {
      expect(getListTransformCards(Immutable.fromJS({startIndex: 0, endIndex: 1}))).to.eql(
        Immutable.fromJS([{type: 'single', single: {startIndex: { value: 0 }}}])
      );
    });

    it('should return multiple index cards if multiple selected', () => {
      const cards = getListTransformCards(Immutable.fromJS({startIndex: 0, endIndex: 2}));
      expect(cards.size).to.equal(4);
      expect(cards.get(0)).to.eql(Immutable.fromJS({
        type: 'multiple',
        multiple: {
          startIndex: {
            value: 0,
            direction: 'Start'
          },
          endIndex: {
            value: 1, // form values are inclusive
            direction: 'Start'
          }
        }
      }));
    });
  });
});

