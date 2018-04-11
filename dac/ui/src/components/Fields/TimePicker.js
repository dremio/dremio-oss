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
import moment from 'moment';
import { CELL_EXPANSION_HEADER, WHITE } from 'uiTheme/radium/colors';
import { dateTypeToFormat } from 'constants/DataTypes';

const TIME_FORMAT = 'HH:mm:ss';

export default class TimePicker extends Component {
  static propTypes = {
    value: PropTypes.oneOfType([
      PropTypes.number,
      PropTypes.string
    ]),
    columnType: PropTypes.string,
    onBlur: PropTypes.func
  }
  static defaultProps = {
    value: '00:00:00'
  }
  constructor(props) {
    super(props);

    this.state = { value: moment(props.value, dateTypeToFormat[props.columnType]).format(TIME_FORMAT) };
  }

  componentWillReceiveProps(nextProps) {
    this.setState({
      value: moment(nextProps.value, dateTypeToFormat[nextProps.columnType]).format(TIME_FORMAT)
    });
  }

  onChange = (e) => {
    const { value } = e.target;
    this.setState({
      value
    });
  }
  onBlur = () => {
    const { value } = this.state;
    const valMoment = moment(value, TIME_FORMAT);
    if (valMoment.isValid()) {
      this.props.onBlur(valMoment);
    }
  }
  render() {
    const { value } = this.state;
    return (
      <div className='field' style={styles.picker}>
        <input
          type='text'
          style={styles.input}
          value={value}
          onChange={this.onChange}
          onBlur={this.onBlur}
        />
        <div style={{ margin: 3 }}>
          hours : minutes : seconds
        </div>
      </div>
    );
  }
}

const styles = {
  picker: {
    display: 'flex',
    alignItems: 'center',
    background: CELL_EXPANSION_HEADER,
    borderRadius: '1px 1px 0px 0px',
    fontFamily: 'Roboto-Regular',
    fontSize: 11,
    color: '#999999',
    height: 30,
    margin: 3
  },
  input: {
    background: WHITE,
    border: '1px solid rgba(0,0,0,0.10)',
    borderRadius: 2
  }
};
