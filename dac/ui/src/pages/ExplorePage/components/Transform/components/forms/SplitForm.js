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
import { connectComplexForm } from 'components/Forms/connectComplexForm';
import Immutable from 'immutable';
import { pick } from 'lodash/object';
import NewFieldSection from 'components/Forms/NewFieldSection';
import { getTransformCards } from 'selectors/transforms';
import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import exploreUtils from 'utils/explore/exploreUtils';
import TransformForm, { formWrapperProps } from '../../../forms/TransformForm';
import SplitFooter from './../SplitFooter';
import SplitCards from './sections/SplitCards';

const SECTIONS = [SplitCards, SplitFooter, NewFieldSection];

const DEFAULT_CARD = {
  type: 'split',
  rule: {
    matchType: 'exact',
    ignoreCase: false
  }
};

export class SplitForm extends Component {

  static propTypes = {
    submit: PropTypes.func,
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    cards: PropTypes.instanceOf(Immutable.List),
    fields: PropTypes.object,
    loadTransformCardPreview: PropTypes.func,
    transform: PropTypes.instanceOf(Immutable.Map),
    dataset: PropTypes.instanceOf(Immutable.Map),
    hasSelection: PropTypes.bool
  };

  static defaultProps = {
    cards: Immutable.fromJS([{}])
  };

  submit = (values, submitType) => {
    return this.props.submit({
      ...fieldsMappers.getCommonValues(values, this.props.transform),
      fieldTransformation: {
        type: 'Split',
        rule: fieldsMappers.getRuleFromCards(values.cards, values.activeCard),
        ...fieldsMappers.getSplitPosition(values)
      }
    }, submitType);
  };

  render() {
    const { fields, cards, loadTransformCardPreview, hasSelection } = this.props;

    return (
      <TransformForm
        {...formWrapperProps(this.props)}
        onFormSubmit={this.submit}
        loadTransformCardPreview={loadTransformCardPreview}>
        <div>
          <SplitCards
            cards={cards}
            fields={fields}
            hasSelection={hasSelection}
          />
          <SplitFooter fields={fields}/>
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
      cards,
      initialValues: {
        cards: cards.toJS().map((card) => pick(card, ['rule', 'type'])),
        newFieldName: columnName,
        dropSourceField: true,
        activeCard: 0,
        position: 'First',
        index: 0,
        maxFields: 10
      }
    };
  }
}

export default connectComplexForm({
  form: 'splitForm'
}, SECTIONS, mapStateToProps, null)(SplitForm);
