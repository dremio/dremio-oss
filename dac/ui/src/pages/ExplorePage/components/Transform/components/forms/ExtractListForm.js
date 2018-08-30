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
import { Component } from 'react';
import PropTypes from 'prop-types';
import Immutable from 'immutable';
import { pick } from 'lodash/object';
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import { getTransformCards } from 'selectors/transforms';
import { sectionMargin } from '@app/uiTheme/less/layout.less';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ExtractListCards from './sections/ExtractListCards';

const SECTIONS = [ExtractListCards, NewFieldSection];

const DEFAULT_CARD = {
  type: 'single'
};

export class ExtractListForm extends Component {
  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    values: PropTypes.object,
    onFormChange: PropTypes.func,
    loadTransformCardPreview: PropTypes.func
  };

  static defaultProps = {
    cards: Immutable.fromJS([{}])
  };

  submit = (values, submitType) => this.props.submit({
    ...fieldsMappers.getCommonValues(values, this.props.transform),
    fieldTransformation: {
      type: 'ExtractList',
      rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
    }
  }, submitType);

  render() {
    const { fields, cards, loadTransformCardPreview } = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        loadTransformCardPreview={loadTransformCardPreview}>
        <div>
          <ExtractListCards
            cards={cards}
            fields={fields}/>
          <NewFieldSection fields={fields} className={sectionMargin} />
        </div>
      </TransformForm>
    );
  }
}

export function getListTransformCards(selection) {
  if (!selection || selection.isEmpty()) {
    return Immutable.fromJS([DEFAULT_CARD]);
  }
  // endIndex here is exclusive
  const { startIndex, endIndex, listLength } = selection.toJS();

  if (startIndex === endIndex - 1) {
    return Immutable.fromJS([{
      type: 'single',
      single: {
        startIndex: {
          value: startIndex
        }
      }
    }]);
  }
  // NOTE: form field endIndex is inclusive, so need to switch it here.
  return Immutable.fromJS([{
    type: 'multiple',
    multiple: {
      startIndex: { value: startIndex, direction: 'Start'},
      endIndex: { value: endIndex - 1, direction: 'Start'}
    }
  }, {
    type: 'multiple',
    multiple: {
      startIndex: { value: startIndex, direction: 'Start'},
      endIndex: { value: listLength - endIndex, direction: 'End'}
    }
  }, {
    type: 'multiple',
    multiple: {
      startIndex: { value: listLength - startIndex - 1, direction: 'End'},
      endIndex: { value: endIndex - 1, direction: 'Start'}
    }
  }, {
    type: 'multiple',
    multiple: {
      startIndex: { value: listLength - startIndex - 1, direction: 'End'},
      endIndex: { value: listLength - endIndex, direction: 'End'}
    }
  }]);
}

function mapStateToProps(state, { transform }) {
  const columnName = transform.get('columnName');

  // serverCards contains matching counts and examples for each card
  const serverCards = getTransformCards(state, transform, DEFAULT_CARD);
  // cardsFields contains form fields for each card.
  const cardsFields = getListTransformCards(transform.get('selection'));

  return {
    cards: serverCards,
    initialValues: {
      newFieldName: columnName,
      dropSourceField: false,
      cards: cardsFields.toJS().map((card) => pick(card, ['multiple', 'single', 'type'])),
      activeCard: 0
    }
  };
}

export default connectComplexForm({
  form: 'extractList'
}, SECTIONS, mapStateToProps, null)(ExtractListForm);
