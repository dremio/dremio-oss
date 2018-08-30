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
import classNames from 'classnames';
import { PrevalidatedTextField } from 'components/Fields';
import FieldWithError from 'components/Fields/FieldWithError';
import Checkbox from 'components/Fields/Checkbox';
import { applyValidators, isRequired } from 'utils/validation';
import { base, text, row } from './NewFieldSection.less';

export default class NewFieldSection extends Component {

  static getFields() {
    return ['newFieldName', 'dropSourceField'];
  }

  static getInitialValues() {
    return {
      dropSourceField: true
    };
  }

  static propTypes = {
    style: PropTypes.object,
    columnName: PropTypes.string,
    fields: PropTypes.object,
    showDropSource: PropTypes.bool,
    className: PropTypes.string
  };

  static defaultProps = {
    showDropSource: true
  }

  static validate(values) {
    return applyValidators(values, [isRequired('newFieldName', la('New Field Name'))]);
  }

  componentWillMount() {
    const {dropSourceField} = this.props.fields;
    this.managePostfix(dropSourceField.value);
  }

  handleCheckboxChange = (e) => {
    const {dropSourceField} = this.props.fields;
    this.managePostfix(e.target.checked);
    dropSourceField.onChange(e.target.checked);
  }

  managePostfix(checked) {
    if (!this.props.showDropSource) return;

    const {newFieldName} = this.props.fields;
    const sourceColumnName = this.getSourceColumnName();
    if (!checked && newFieldName.value === sourceColumnName) {
      newFieldName.onChange(`${sourceColumnName}_1`);
    } else if (checked && newFieldName.value === `${sourceColumnName}_1`) {
      newFieldName.onChange(sourceColumnName);
    }
  }

  getSourceColumnName() {
    const { fields: { newFieldName }, columnName } = this.props;
    return columnName || (newFieldName && newFieldName.initialValue);
  }

  render() { // todo: loc
    const {
      fields: { newFieldName, dropSourceField },
      style,
      showDropSource,
      className
    } = this.props;

    // use a PrevalidatedTextField so that the value isn't sent as-typing -
    // only hitting the server on blur
    return (
      <div className={classNames(base, className)} style={style}>
        <FieldWithError
          {...newFieldName}
          errorPlacement='bottom'
          label={la('New Field Name')}>
          <PrevalidatedTextField
            {...newFieldName}
            className={text}/>
        </FieldWithError>
        { showDropSource && <FieldWithError
          label={la('Options')}
          {...dropSourceField}>
          <Checkbox
            {...dropSourceField}
            onChange={this.handleCheckboxChange}
            label={`Drop Source Field (${this.getSourceColumnName()})`}
            className={row}/>
        </FieldWithError> }
      </div>
    );
  }
}
