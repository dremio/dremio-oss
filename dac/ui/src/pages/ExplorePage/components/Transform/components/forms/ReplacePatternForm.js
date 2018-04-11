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
import { getTransformCards } from 'selectors/transforms';
import NewFieldSection from 'components/Forms/NewFieldSection';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';
import exploreUtils from 'utils/explore/exploreUtils';

import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import ReplaceFooter from './../ReplaceFooter';
import ReplacePatternCards from './sections/ReplacePatternCards';

const SECTIONS = [ReplacePatternCards, NewFieldSection, ReplaceFooter];

const DEFAULT_CARD = {
  type: 'replace',
  replace: {
    selectionPattern: '',
    selectionType: 'CONTAINS'
  }
};

export class ReplacePatternForm extends Component {
  static propTypes = {
    transform: PropTypes.instanceOf(Immutable.Map),
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    submitForm: PropTypes.func,
    loadTransformCardPreview: PropTypes.func,
    dataset: PropTypes.instanceOf(Immutable.Map),
    hasSelection: PropTypes.bool
  };

  static defaultProps = {
    cards: Immutable.fromJS([{}])
  };

  submit = (values, submitType) => {
    const transformType = this.props.transform.get('transformType');
    const data = transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, this.props.transform),
        fieldTransformation: {
          type: 'ReplacePattern',
          replaceType: fieldsMappers.getReplaceType(values),
          replacementValue: fieldsMappers.getReplacementValue(values),
          rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, this.props.transform),
        filter: filterMappers.mapFilterExcludePattern(values, this.props.transform)
      };
    return this.props.submit(data, submitType);
  }

  render() {
    const { transform, fields, cards, submitForm, loadTransformCardPreview, hasSelection } = this.props;

    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        loadTransformCardPreview={loadTransformCardPreview}>
        <div>
          <ReplacePatternCards
            cards={cards}
            fields={fields}
            hasSelection={hasSelection}
          />
          {transform.get('transformType') === 'replace' &&
            <ReplaceFooter
              tabId='replace'
              transform={transform}
              fields={fields}
              submitForm={submitForm}
            />}
        </div>
      </TransformForm>
    );
  }
}

function mapStateToProps(state, { transform }) {
  const columnName = transform.get('columnName');
  const cards = getTransformCards(state, transform, DEFAULT_CARD);
  const hasSelection = exploreUtils.transformHasSelection(transform);

  if (cards && cards.size > 0) {
    return {
      hasSelection,
      columnName,
      cards,
      initialValues: {
        newFieldName: columnName,
        dropSourceField: true,
        activeCard: 0,
        cards: cards.toJS().map((card) => pick(card, ['replace', 'type'])),
        replacementValue: '',
        replaceSelectionType: 'VALUE',
        replaceType: 'VALUE'
      }
    };
  }
}

export default connectComplexForm({
  form: 'replacePattern'
}, SECTIONS, mapStateToProps, null)(ReplacePatternForm);
