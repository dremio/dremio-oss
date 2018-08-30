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
import {padStart} from 'lodash/string';

import PropTypes from 'prop-types';
import { injectIntl, FormattedMessage } from 'react-intl';

import { FormatField, Checkbox } from 'components/Fields';
import { label, divider } from 'uiTheme/radium/forms';
import {INVISIBLE_CHARS} from 'utils/dataFormatUtils';

@injectIntl
export default class TextFormatForm extends Component {

  static propTypes = {
    fields: PropTypes.object,
    disabled: PropTypes.bool,
    intl: PropTypes.object.isRequired
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
      const escapeForJSON = str => {
        return str.replace(
          /[^\\]/gi,
          match => JSON.stringify(match).slice(1, -1)
        );
      };
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
    return JSON.stringify(value).slice(1, -1).replace(/\\"/g, '"').split('').map(c => (INVISIBLE_CHARS.has(c) && `\\u${padStart(c.charCodeAt(0).toString(16), 4, '0')}`) || c).join('');
  }

  render() {
    const {
      disabled,
      intl,
      fields: {
        Text: {fieldDelimiter, quote, comment, lineDelimiter, escape, extractHeader, trimHeader, skipFirstLine}
      }
    } = this.props;

    const fieldProps = (field, last) => {
      return {
        ...field,
        onBlur: this.onDelimiterChange(field),
        onChange: this.onDelimiterChange(field),
        value: this.getDelimiterValue(field),
        style: last ? styles.lastField : styles.field,
        disabled
      };
    };
    const fieldDelimiterOptions = [
      {option: ',', label: intl.formatMessage({ id: 'File.Comma' })}, {option: '\\t', label: intl.formatMessage({ id: 'File.Tab' })}, {option: '|', label: intl.formatMessage({ id: 'File.Pipe' })}
    ];
    const quoteOptions = [
      {option: '"', label: intl.formatMessage({ id: 'File.DoubleQuote' })}, {option: "'", label: intl.formatMessage({ id: 'File.SingleQuote' })}
    ];
    const commentOptions = [
      {option: '#', label: intl.formatMessage({ id: 'File.NumberSign' })}, {option: '//', label: intl.formatMessage({ id: 'File.DoubleSlash' })}
    ];
    const lineDelimiterOptions = [
      {option: '\\r\\n', label: intl.formatMessage({ id: 'File.WindowsDelimiter' })}, {option: '\\n', label: intl.formatMessage({ id: 'File.LinuxDelimiter' })}
    ];
    const escapeOptions = [
      {option: '"', label: intl.formatMessage({ id: 'File.DoubleQuote' })},
      {option: '`', label: intl.formatMessage({ id: 'File.BackQuote' })},
      {option: '\\', label: intl.formatMessage({ id: 'File.Backslash' })}
    ];
    return (
      <div>
        <div style={styles.row}>
          <FormatField
            {...fieldProps(fieldDelimiter)}
            label={intl.formatMessage({ id: 'File.FieldDelimiter' })}
            options={fieldDelimiterOptions}
          />
          <FormatField
            {...fieldProps(quote)}
            label={intl.formatMessage({ id: 'File.Quote' })}
            options={quoteOptions}
          />
          <FormatField
            {...fieldProps(comment, true)}
            label={intl.formatMessage({ id: 'File.Comment' })}
            options={commentOptions}
          />
        </div>
        <hr style={divider}/>
        <div style={styles.row}>
          <FormatField
            {...fieldProps(lineDelimiter)}
            label={intl.formatMessage({ id: 'File.LineDelimiter' })}
            options={lineDelimiterOptions}
          />
          <FormatField
            {...fieldProps(escape)}
            label={intl.formatMessage({ id: 'File.Escape' })}
            options={escapeOptions}
          />
          <div style={styles.lastField}>
            <label style={label}><FormattedMessage id='Common.Options' /></label>
            <div style={styles.options}>
              <Checkbox disabled={disabled} style={styles.checkbox} dataQa='extract-field-names'
                label={intl.formatMessage({ id: 'File.ExtractFieldNames' })} {...extractHeader}/>
              <Checkbox disabled={disabled} style={styles.checkbox} dataQa='skip-first-line'
                label={intl.formatMessage({ id: 'File.SkipFirstLine' })} {...skipFirstLine}/>
            </div>
            <div style={styles.options}>
              <Checkbox
                disabled={disabled}
                style={styles.checkbox}
                label={intl.formatMessage({ id: 'File.TrimFieldNames' })}
                {...trimHeader}
              />
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
    flex: 1,
    marginRight: 10
  },
  lastField: {
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
