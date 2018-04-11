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
import Radium from 'radium';

import PropTypes from 'prop-types';

import Radio from 'components/Fields/Radio';

import { formLabel } from 'uiTheme/radium/typography';

@Radium
export default class ConvertCaseOptions extends Component {
  static propTypes = {
    fields: PropTypes.object
  }

  static getFields() {
    return ['action'];
  }

  static getInitialValues() {
    return {
      action: 'UPPERCASE'
    };
  }

  constructor(props) {
    super(props);
  }

  render() {
    const { fields: { action } } = this.props;
    return (
      <div style={[styles.base]}>
        <h3 style={[styles.title, formLabel]}>{la('Options')}</h3>
        <div style={[styles.items]}>
          <Radio
            radioValue='UPPERCASE'
            label='UPPERCASE'
            style={styles.radio}
            {...action}/>
          <Radio
            radioValue='lowercase'
            label='lowercase'
            style={styles.radio}
            {...action}/>
          <Radio
            radioValue='TITLECASE'
            label='Title Case'
            style={styles.radio}
            {...action}/>
        </div>
      </div>
    );
  }
}

const styles = {
  base: {
    marginLeft: 15,
    marginTop: 10,
    marginBottom: 5
  },
  items: {
    display: 'flex',
    flexDirection: 'column'
  },
  radio: {
    marginTop: 10
  }
};
