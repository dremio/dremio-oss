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
import { getTransformCards } from 'selectors/transforms';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import exploreUtils from 'utils/explore/exploreUtils';
import { sectionMargin } from '@app/uiTheme/less/layout.less';

import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ExtractTextCards from './sections/ExtractTextCards';

const SECTIONS = [ExtractTextCards, NewFieldSection];

const DEFAULT_CARD = {
  type: 'position'
};

export class ExtractTextForm extends Component {
  static propTypes = {
    // needed only for submit handler
    transform: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func.isRequired,
    onCancel: PropTypes.func.isRequired,
    dataset: PropTypes.instanceOf(Immutable.Map),

    // connected
    hasSelection: PropTypes.bool,
    columnName: PropTypes.string,
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
      type: 'extract',
      rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
    }
  }, submitType);

  render() {
    const { fields, cards, columnName, loadTransformCardPreview, hasSelection } = this.props;
    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        loadTransformCardPreview={loadTransformCardPreview}>
        <div>
          <ExtractTextCards
            hasSelection={hasSelection}
            columnName={columnName}
            cards={cards}
            fields={fields}/>
          <NewFieldSection columnName={columnName} fields={fields} className={sectionMargin} style={{ marginBottom: 10 }}/>
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, { transform }) {
  const cards = getTransformCards(state, transform, DEFAULT_CARD);
  const hasSelection = exploreUtils.transformHasSelection(transform);
  if (cards && cards.size > 0) {
    return {
      hasSelection,
      cards,
      columnName: transform.get('columnName'),
      initialValues: {
        newFieldName: transform.get('columnName') + '_1', // as dropSourceField = true need provide different name
        cards: cards.toJS().map((card) => pick(card, ['pattern', 'position', 'type'])),
        dropSourceField: false,
        activeCard: 0
      }
    };
  }
}

export default connectComplexForm({
  form: 'extractText'
}, SECTIONS, mapStateToProps, null)(ExtractTextForm);
