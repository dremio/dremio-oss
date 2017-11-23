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
import { Component } from 'react';

import PropTypes from 'prop-types';

import FieldWithError from 'components/Fields/FieldWithError';
import TextField from 'components/Fields/TextField';
import Checkbox from 'components/Fields/Checkbox';
import HoverHelp from 'components/HoverHelp';
import { applyValidators, isNumber } from 'utils/validation';
import { section, sectionTitle, formRow } from 'uiTheme/radium/forms';

import SourceProperties from './SourceProperties';


const fieldHelp = {
  authDatabase: 'Default is "admin"',
  useSsl: 'Force encrypted connection over SSL',
  authenticationTimeoutMillis: 'Default is 2000 ms',
  secondaryReadsOnly: `Queries will fail if no secondaries are available.
                       Use this setting to avoid impact on the primary replica.`,
  subpartitionSize: `Number of records to be read by query fragments
  (increases query parallelism). Ignored if value is zero.`
};

export default class MongoDbOptions extends Component {
  static getFields() {
    return [
      'config.replicaSet',
      'config.useSsl',
      'config.authDatabase',
      'config.authenticationTimeoutMillis',
      'config.secondaryReadsOnly',
      'config.subpartitionSize',
      'config.propertyList[].name',
      'config.propertyList[].value'
    ].concat(SourceProperties.getFields());
  }

  static propTypes = {
    replicaSet: PropTypes.string,
    useSsl: PropTypes.bool,
    authDatabase: PropTypes.string,
    propertyList: PropTypes.array,
    fields: PropTypes.object
  };

  static validate(values) {

    return {
      ...SourceProperties.validate(values),
      ...applyValidators(values, [
        isNumber('config.authenticationTimeoutMillis', 'authenticationTimeoutMillis'),
        isNumber('config.subpartitionSize', 'subpartitionSize')
      ])
    };
  }

  constructor(props) {
    super(props);
  }

  render() {
    const {fields} = this.props;
    const { config: {
      useSsl,
      authDatabase,
      authenticationTimeoutMillis,
      secondaryReadsOnly,
      subpartitionSize,
      propertyList,
      authenticationType
    }} = fields;
    return (
      <div>
        <div style={section}>
          <div style={{marginTop: -20}}>
            <FieldWithError label={la('Authentication Database')} {...authDatabase} style={styles.fieldWithHelp}>
              <TextField disabled={authenticationType.value !== 'MASTER'} {...authDatabase}/>
            </FieldWithError>
            <HoverHelp
              content={fieldHelp.authDatabase}
              style={styles.hoverHelp}/>
          </div>
        </div>
        <div style={section}>
          <h2 style={sectionTitle}>{la('Data Access')}</h2>

          <div style={styles.checkboxWithHelp}>
            <Checkbox {...useSsl} label={la('Require SSL Encryption')}/>
            <HoverHelp
              content={fieldHelp.useSsl}
              style={styles.hoverHelp}/>
          </div>

          <div style={styles.checkboxWithHelp}>
            <Checkbox {...secondaryReadsOnly} label={la('Read from secondaries only')} />
            <HoverHelp
              content={fieldHelp.secondaryReadsOnly}
              style={styles.hoverHelp}/>
          </div>

          <div style={formRow}>
            <FieldWithError
              label={la('Auth Timeout (millis)')}
              {...authenticationTimeoutMillis}
              style={styles.fieldWithHelp}>
              <TextField {...authenticationTimeoutMillis}/>
            </FieldWithError>
            <HoverHelp
              content={fieldHelp.authenticationTimeoutMillis}
              style={styles.tallHoverHelp}/>
          </div>

          <div style={formRow}>
            <FieldWithError
              label={la('Subpartition Size')}
              {...subpartitionSize}
              style={styles.fieldWithHelp}>
              <TextField {...subpartitionSize}/>
            </FieldWithError>
            <HoverHelp
              content={fieldHelp.subpartitionSize}
              style={styles.tallHoverHelp}/>
          </div>
        </div>
        <div style={section}>
          <FieldWithError {...propertyList}>
            <SourceProperties
              title={la('Connection String')}
              emptyLabel={la('(No Options Added)')}
              addLabel={la('Add Option')}
              fields={fields}/>
          </FieldWithError>
        </div>
      </div>
    );
  }
}


const styles = {
  hoverHelp: {
    display: 'inline-block',
    top: 8,
    left: 5
  },
  tallHoverHelp: {
    display: 'inline-block',
    top: 10,
    left: 5
  },
  checkboxWithHelp: {
    ...formRow,
    marginTop: -10,
    display: 'flex',
    alignItems: 'baseline'
  },
  fieldWithHelp: {
    display: 'inline-block'
  }
};
