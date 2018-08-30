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

import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import { sectionTitle, description as descriptionStyle } from 'uiTheme/radium/forms';
import { applyValidators, isRequired, isWholeNumber } from 'utils/validation';
import { rowOfInputsSpacing } from '@app/uiTheme/less/forms.less';

export default class Host extends Component {
  static getFields() {
    return [
      'id', 'hostname', 'port'
    ];
  }

  static propTypes = {
    fields: PropTypes.object,
    style: PropTypes.object,
    title: PropTypes.string,
    description: PropTypes.string
  };

  static validate(values) {
    return applyValidators(values, [
      isRequired('hostname'),
      isRequired('port'),
      isWholeNumber('port')
    ]);
  }

  render() {
    const {style, fields: {hostname, port}} = this.props;
    const title = this.props.title ? <div style={styles.title}>{this.props.title}</div> : null;
    const description = this.props.description ? <div className='largerFontSize' style={styles.des}>{this.props.description}</div> : null;
    return (
      <div className={rowOfInputsSpacing} style={{...styles.hostPortItem, ...style}}>
        {title && <h2>{title}</h2>}
        {description}
        <FieldWithError errorPlacement='top' label='Host' {...hostname} style={styles.hostPortField}>
          <TextField {...hostname} style={styles.textFieldItem} />
        </FieldWithError>
        <FieldWithError errorPlacement='top' label='Port' {...port} style={styles.hostPortField}>
          <TextField {...port} style={styles.textFieldItem} />
        </FieldWithError>
      </div>
    );
  }
}

const styles = {
  hostPortItem: {
    display: 'flex',
    width: '100%',
    marginBottom: 12
  },
  hostPortField: {
    flex: '1 1 auto'
  },
  textFieldItem: {
    width: '100%'
  },
  title: {
    ...sectionTitle,
    margin: '10px 0'
  },
  des: {
    ...descriptionStyle,
    margin: '10px 0'
  }
};
