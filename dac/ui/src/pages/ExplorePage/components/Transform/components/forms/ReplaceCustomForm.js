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
import { connectComplexForm } from 'components/Forms/connectComplexForm';

import fieldsMappers from 'utils/mappers/ExplorePage/Transform/fieldsMappers';
import filterMappers from 'utils/mappers/ExplorePage/Transform/filterMappers';
import NewFieldSection from 'components/Forms/NewFieldSection';
import exploreUtils from 'utils/explore/exploreUtils';
import TransformForm, {formWrapperProps} from '../../../forms/TransformForm';
import ReplaceFooter from './../ReplaceFooter';
import CustomCondition from './../ContentWithoutCards/CustomCondition';

const SECTIONS = [ReplaceFooter, NewFieldSection];

export class ReplaceCustomForm extends Component {

  static propTypes = {
    submit: PropTypes.func,
    transform: PropTypes.instanceOf(Immutable.Map),
    onCancel: PropTypes.func,
    changeFormType: PropTypes.func,
    fields: PropTypes.object,
    type: PropTypes.string,
    tryToLoadNewCard: PropTypes.func,
    columnType: PropTypes.string,
    loadTransformValuesPreview: PropTypes.func,
    submitForm: PropTypes.func,
    dataset: PropTypes.instanceOf(Immutable.Map)
  };

  constructor(props) {
    super(props);
  }

  submit = (values, submitType) => {
    const transformType = this.props.transform.get('transformType');
    const data = transformType === 'replace'
      ? {
        ...fieldsMappers.getCommonValues(values, this.props.transform),
        fieldTransformation: {
          type: 'ReplaceCustom',
          ...fieldsMappers.getReplaceCustom(values)
        }
      }
      : {
        ...filterMappers.getCommonFilterValues(values, this.props.transform),
        filter: filterMappers.mapFilterExcludeCustom(values, this.props.transform)
      };

    return this.props.submit(data, submitType);
  }

  renderFooter() {
    const {fields, transform, submitForm} = this.props;
    return transform.get('transformType') === 'replace'
    ? <ReplaceFooter
      tabId='replace'
      fields={fields}
      submitForm={submitForm}
      transform={transform}/>
    : null;
  }

  render() {
    const {fields, submitForm} = this.props;
    return (
      <TransformForm {...formWrapperProps(this.props)} onFormSubmit={this.submit}>
        <div className='transform-selection' style={styles.selections}>
          <CustomCondition submitForm={submitForm} fields={fields}/>
        </div>
        {this.renderFooter()}
      </TransformForm>
    );
  }
}

function mapStateToProps(state, props) {
  const columnName = props.transform.get('columnName');
  const escapedColumnName = exploreUtils.escapeFieldNameForSQL(columnName);
  return {
    initialValues: {
      newFieldName: columnName,
      dropSourceField: true,
      replacementValue: '',
      replaceType: 'VALUE',
      booleanExpression: `${escapedColumnName} IS NOT NULL \nOR\n${escapedColumnName} IS NULL`
    }
  };
}

export default connectComplexForm({
  form: 'replaceCustom',
  fields: ['booleanExpression']
}, SECTIONS, mapStateToProps, null)(ReplaceCustomForm);

const styles = {
  selections: {
    marginBottom: 5
  }
};
