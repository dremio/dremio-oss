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

import { formLabel } from 'uiTheme/radium/typography';
import Radio from 'components/Fields/Radio';

import Radium from 'radium';

import PropTypes from 'prop-types';

@Radium
export default class TrimWhiteSpaceOptions extends Component {
  static getInitialValues() {
    return {
      action: 'BOTH'
    };
  }

  static propTypes = {
    handleTypeChange: PropTypes.func,
    type: PropTypes.string,
    fields: PropTypes.object
  };

  constructor(props) {
    super(props);
    this.handleRadioChange = this.handleRadioChange.bind(this);
  }

  handleRadioChange(e) {
    this.props.handleTypeChange(e.target.value);
  }

  render() {
    const { fields: { action } } = this.props;
    return (
      <div style={[styles.base]}>
        <h3 style={[styles.title, formLabel]}>{la('Options')}</h3>
        <div style={[styles.items]}>
          <Radio
            {...action}
            radioValue='BOTH'
            label='Trim from both sides'
            style={styles.radio}/>
          <Radio
            {...action}
            radioValue='LEFT'
            label='Trim from the start (Trim Left)'
            style={styles.radio}/>
          <Radio
            {...action}
            radioValue='RIGHT'
            label='Trim from the end (Trim Right)'
            style={styles.radio}/>
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
