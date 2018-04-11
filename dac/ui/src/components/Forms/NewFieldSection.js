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
import Radium from 'radium';

import { formLabel } from 'uiTheme/radium/typography';
import { PrevalidatedTextField } from 'components/Fields';
import FieldWithError from 'components/Fields/FieldWithError';
import Checkbox from 'components/Fields/Checkbox';
import { applyValidators, isRequired } from 'utils/validation';


@Radium
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
    showDropSource: PropTypes.bool
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
    const { fields: { newFieldName, dropSourceField }, style, showDropSource } = this.props;

    // use a PrevalidatedTextField so that the value isn't sent as-typing -
    // only hitting the server on blur
    return (
      <div style={[styles.base, style]}>
        <FieldWithError
          {...newFieldName}
          errorPlacement='bottom'
          label={la('New Field Name')}
          labelStyle={styles.label}
          style={formLabel}>
          <PrevalidatedTextField
            {...newFieldName}
            style={styles.text}/>
        </FieldWithError>
        { showDropSource && <FieldWithError
          label={la('Options')}
          {...dropSourceField}
          labelStyle={styles.label}
          style={formLabel}>
          <Checkbox
            {...dropSourceField}
            onChange={this.handleCheckboxChange}
            label={`Drop Source Field (${this.getSourceColumnName()})`}
            style={styles.checkbox}/>
        </FieldWithError> }
      </div>
    );
  }
}

const styles = {
  base: {
    display: 'flex',
    alignItems: 'center',
    marginLeft: 10,
    marginBottom: 10,
    paddingTop: 5
  },
  text: {
    width: 200,
    height: 28
  },
  label: {
    marginBottom: 0
  },
  checkbox: {
    marginTop: 1
  }
};
