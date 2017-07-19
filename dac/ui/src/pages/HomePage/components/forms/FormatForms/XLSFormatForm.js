/*
 * Copyright (C) 2017 Dremio Corporation
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
import { Component, PropTypes } from 'react';

import { Checkbox, TextField, FieldWithError } from 'components/Fields';
import { divider } from 'uiTheme/radium/forms';

export default class XLSFormatForm extends Component {

  static propTypes = {
    fields: PropTypes.object,
    disabled: PropTypes.bool
  };

  static getFields() {
    return ['extractHeader', 'hasMergedCells', 'sheetName'];
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {
      disabled,
      fields: { XLS: { extractHeader, hasMergedCells, sheetName }}
      } = this.props;

    return (
      <div>
        <div style={styles.row}>
          <div style={styles.options}>
            <Checkbox disabled={disabled} style={styles.checkbox} dataQa='extract-field-names'
              label={la('Extract Field Names')} {...extractHeader}/>
          </div>
          <div style={styles.options}>
            <Checkbox
              disabled={disabled}
              style={styles.checkbox}
              label={la('Expand Merged Cells')}
              {...hasMergedCells}
            />
          </div>
        </div>
        <div style={styles.row}>
          <FieldWithError label={la('Sheet Name')} {...sheetName}>
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
