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
import { formRow } from 'uiTheme/radium/forms';
import { FieldWithError, TextField } from 'components/Fields';
import HoverHelp from 'components/HoverHelp';
import { applyValidators, isWholeNumber } from 'utils/validation';

// todo: loc
const fieldHelp = {
  fetchSize: 'Number of records to fetch at once. Set to 0 to have Dremio automatically decide.'
};
const DEFAULT_FETCH_SIZE = 0;

export default class JDBCOptions extends Component {

  static propTypes = {
    fields: PropTypes.object.isRequired
  };

  static mapStateToProps(state, props) {
    const initialValues = {
      ...props.initialValues,
      config: {
        fetchSize: DEFAULT_FETCH_SIZE,
        ...props.initialValues.config
      }
    };
    return {
      initialValues
    };
  }


  static getFields() {
    return [
      'config.fetchSize'
    ];
  }

  static validate(values) {
    return applyValidators(values, [
      isWholeNumber('config.fetchSize', la('Records Fetch Size'))
    ]);
  }

  render() {
    const { fields: { config: { fetchSize }}} = this.props;
    return (
      <div style={formRow}>
        <FieldWithError
          label={la('Record Fetch Size')}
          {...fetchSize}
          style={styles.fieldWithHelp}>
          <TextField {...fetchSize}/>
        </FieldWithError>
        <HoverHelp
          content={fieldHelp.fetchSize}
          style={styles.hoverHelp}/>

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
  fieldWithHelp: {
    display: 'inline-block'
  }
};
