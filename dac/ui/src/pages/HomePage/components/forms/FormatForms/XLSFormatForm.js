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
import { injectIntl } from 'react-intl';

import { Checkbox, TextField, FieldWithError } from 'components/Fields';
import { divider } from 'uiTheme/radium/forms';

@injectIntl
export default class XLSFormatForm extends Component {
  static propTypes = {
    fields: PropTypes.object,
    disabled: PropTypes.bool,
    intl: PropTypes.object.isRequired
  };

  static getFields() {
    return ['extractHeader', 'hasMergedCells', 'sheetName'];
  }

  render() {
    const {
      disabled,
      intl,
      fields: { XLS: { extractHeader, hasMergedCells, sheetName }}
      } = this.props;

    return (
      <div>
        <div style={styles.row}>
          <div style={styles.options}>
            <Checkbox disabled={disabled} style={styles.checkbox} dataQa='extract-field-names'
              label={intl.formatMessage({ id: 'File.ExtractFieldNames' })} {...extractHeader}/>
          </div>
          <div style={styles.options}>
            <Checkbox
              disabled={disabled}
              style={styles.checkbox}
              label={intl.formatMessage({ id: 'File.ExpandMergedCells' })}
              {...hasMergedCells}
            />
          </div>
        </div>
        <div style={styles.row}>
          <FieldWithError label={intl.formatMessage({ id: 'File.SheetName' })} {...sheetName}>
            <TextField {...sheetName} style={styles.textField}/>
          </FieldWithError>
        </div>
        <hr style={divider}/>
      </div>
    );
  }
}

const styles = {
  row: {
    display: 'flex',
    flexWrap: 'wrap',
    marginBottom: 10
  },
  textField: {
    width: 180
  },
  options: {
    display: 'flex',
    alignItems: 'center',
    height: 28
  },
  checkbox: {
    marginRight: 10
  }
};
