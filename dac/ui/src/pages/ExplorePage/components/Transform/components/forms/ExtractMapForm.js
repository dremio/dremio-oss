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
import invariant from 'invariant';
import { pick } from 'lodash/object';

import { connectComplexForm } from 'components/Forms/connectComplexForm';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import { getTransformCards } from 'selectors/transforms';
import { sectionMargin } from '@app/uiTheme/less/layout.less';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ExtractMapCards from './sections/ExtractMapCards';

const SECTIONS = [ExtractMapCards, NewFieldSection];

const DEFAULT_CARD = {
  type: 'map'
};

export class ExtractMapForm extends Component {

  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    dataset: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    onFormChange: PropTypes.func,
    loadTransformCardPreview: PropTypes.func
  };

  static defaultProps = {
    cards: Immutable.fromJS([{}])
  };

  submit = (values, submitType) => this.props.submit({
    ...fieldsMappers.getCommonValues(values, this.props.transform),
    fieldTransformation: {
      type: 'ExtractMap',
      rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
    }
  }, submitType);

  render() {
    const {fields, cards, loadTransformCardPreview} = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        loadTransformCardPreview={loadTransformCardPreview}>
        <div>
          <ExtractMapCards
            cards={cards}
            fields={fields}/>
          <NewFieldSection fields={fields} className={sectionMargin}/>
        </div>
      </TransformForm>
    );
  }
}

export function getExtractMapCards(selection) {
  if (!selection || selection.isEmpty()) {
    return Immutable.fromJS([DEFAULT_CARD]);
  }
  invariant(selection.get('mapPathList') !== undefined, 'must have map selection to get extract map cards');
  return Immutable.fromJS([{
    type: 'map',
    path: selection.get('mapPathList').join('.')
  }]);
}

function mapStateToProps(state, { transform }) {
  const columnName = transform.get('columnName');

  // serverCards contains matching counts and examples for each card
  const serverCards = getTransformCards(state, transform, DEFAULT_CARD);
  // cardsFields contains form fields for each card.
  const fieldCards = getExtractMapCards(transform.get('selection'));

  return {
    cards: serverCards,
    initialValues: {
      newFieldName: columnName,
      dropSourceField: false,
      cards: fieldCards.toJS().map((card) => pick(card, ['path', 'type'])),
      activeCard: 0
    }
  };
}

export default connectComplexForm({
  form: 'extractMap'
}, SECTIONS, mapStateToProps, null)(ExtractMapForm);
