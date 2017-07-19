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

import { FormatField, Checkbox } from 'components/Fields';
import { label, divider } from 'uiTheme/radium/forms';

// todo: loc

export default class TextFormatForm extends Component {

  static propTypes = {
    fields: PropTypes.object,
    disabled: PropTypes.bool
  };

  static getFields() {
    return [
      'fieldDelimiter',
      'quote',
      'comment',
      'lineDelimiter',
      'escape',
      'extractHeader',
      'trimHeader',
      'skipFirstLine'
    ];
  }

  state = {
    invalidJSONFields: {}
  }

  onDelimiterChange = (field) => (evtOrValue) => {
    const value = typeof evtOrValue === 'object' ? evtOrValue.target.value : evtOrValue;
    let newValue = value;

    // try to interpret as JSON so that people can write escape codes,
    // but otherwise fall back to treating as a plain string
    try {

      // treat all backslashes as non-JSON
      // makes the common situation of wanting a single backslash pleasant
      // also avoids auto-replacing "\\" (valid JSON) with "\" thru loopback (making you feel stuck)
      if (newValue.match(/^\\+$/)) {
        throw new Error('treating string of all backslashes as non-JSON');
      }

      // escape things that JSON would want escaped (including reserved chars: http://json.org,
      // and many invisibles - e.g. \u0001).
      // avoiding reverse solidus (backslash) to prevent loopback issues -
      // you'll just have to enter those as valid JSON if you want them in a complex (non-standalone) situation
      // (But normally they'll simply fail JSON.parse and be treated as a plain string.)
      // This makes it so you can combine reserved chars with other valid JSON and have it work.
      // Most esp. wanted to make it so that double-quotes didn't need escaping
      // (they only needs escaping in JSON because double-quotes are in the grammar,
      // but we don't need wrapping quotes here). Esp. since double-quotes are very common.
      const escapeForJSON = str => str.replace(
        /[^\\]/gi,
        match => JSON.stringify(match).slice(1, -1)
      );
      newValue = JSON.parse(`"${escapeForJSON(newValue)}"`);
      this.setState(state => ({invalidJSONFields: {...state.invalidJSONFields, [field.name]: false} }));
    } catch (e) {
      this.setState(state => ({invalidJSONFields: {...state.invalidJSONFields, [field.name]: true} }));
    }
    field.onChange(newValue);
  }

  getDelimiterValue = (field) => {
    const { value } = field;
    if (value.match(/^\\+$/)) { // treating as invalid, see note above
      return value;
    }
    if (this.state.invalidJSONFields[field.name]) return value;
    return JSON.stringify(value).slice(1, -1).replace(/\\"/g, '"');
  }

  render() {
    const {
      disabled,
      fields: {
        Text: {fieldDelimiter, quote, comment, lineDelimiter, escape, extractHeader, trimHeader, skipFirstLine}
      }
    } = this.props;

    const fieldProps = (field) => { // todo: move into FormatField itself?
      return {
        ...field,
        onBlur: this.onDelimiterChange(field),
        onChange: this.onDelimiterChange(field),
        value: this.getDelimiterValue(field),
        style: styles.field,
        disabled
      };
    };

    return (
      <div>
        <div style={styles.row}>
          <FormatField
            {...fieldProps(fieldDelimiter)}
            label={la('Field Delimiter')}
            options={[{option: ',', label: 'Comma'}, {option: '\\t', label: 'Tab'}, {option: '|', label: 'Pipe'}]}
          />
          <FormatField
            {...fieldProps(quote)}
            label={la('Quote')}
            options={[{option: '"', label: 'Double Quote'}, {option: "'", label: 'Single Quote'}]}
          />
          <FormatField
            {...fieldProps(comment)}
            label={la('Comment')}
            options={[{option: '#', label: 'Number Sign'}, {option: '//', label: 'Double Slash'}]}
          />
        </div>
        <hr style={divider}/>
        <div style={styles.row}>
          <FormatField
            {...fieldProps(lineDelimiter)}
            label={la('Line Delimiter')}
            options={[{option: '\\r\\n', label: 'CRLF - Windows'}, {option: '\\n', label: 'LF - Unix/Linux'}]}
          />
          <FormatField
            {...fieldProps(escape)}
            label={la('Escape')}
            options={[
              {option: '"', label: 'Double Quote'},
              {option: '`', label: 'Back Quote'},
              {option: '\\', label: 'Backslash'}
            ]}
          />
          <div style={styles.field}>
            <label style={label}>Options</label>
            <div style={styles.options}>
              <Checkbox disabled={disabled} style={styles.checkbox} dataQa='extract-field-names'
                label={la('Extract Field Names')} {...extractHeader}/>
              <Checkbox disabled={disabled} style={styles.checkbox} dataQa='skip-first-line'
                label={la('Skip First Line')} {...skipFirstLine}/>
            </div>
            <div style={styles.options}>
              <Checkbox disabled={disabled} style={styles.checkbox} label={la('Trim Field Names')} {...trimHeader}/>
            </div>
          </div>
        </div>
      </div>
    );
  }
}

const styles = {
  row: {
    display: 'flex',
    marginTop: 10
  },
  field: {
    flex: 1
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
