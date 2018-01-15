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
import Radium from 'radium';

import PropTypes from 'prop-types';

import FieldValues from 'components/Fields/FieldValues';
import { SECONDARY_BORDER, WHITE } from 'uiTheme/radium/colors';
import { formSectionTitle } from 'uiTheme/radium/exploreTransform';
import './Progress.less';

@Radium
export default class NonMatchingValues extends Component {
  static propTypes = {
    nonMatchingCount: PropTypes.number.isRequired,
    values: PropTypes.array.isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { values } = this.props;
    if (values && values.length === 0) {
      return <div/>;
    }
    return (
      <div className='clean-data-area'>
        <div style={formSectionTitle}>Available Non-matching Values</div>
        <div style={styles.values}>
          <FieldValues options={values}/>
        </div>
      </div>
    );
  }
}

const styles = {
  values: {
    maxWidth: 600,
    height: 215,
    position: 'relative',
    marginTop: 5,
    padding: 5,
    border: `1px solid ${SECONDARY_BORDER}`,
    overflowY: 'scroll',
    backgroundColor: WHITE
  }
};
