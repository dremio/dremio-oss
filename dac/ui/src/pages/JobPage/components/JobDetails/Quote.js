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
import Immutable from 'immutable';
import pureRender from 'pure-render-decorator';
import PropTypes from 'prop-types';
import Radium from 'radium';

import jobsUtils from 'utils/jobsUtils';
import FileUtils from 'utils/FileUtils';
import HoverHelp from 'components/HoverHelp';

@Radium
@pureRender
class Quote extends Component {

  static propTypes = {
    jobIOData: PropTypes.instanceOf(Immutable.Map).isRequired
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { jobIOData } = this.props;
    if (!jobIOData.get('inputBytes') && !jobIOData.get('outputBytes')) {
      return null;
    }

    return (
      <div className='quote-holder' style={[styles.base]}>
        <div style={[styles.input]}>
          <h5>Input</h5>
          <table>
            <tbody>
              <tr className='quote-wrap' style={styles.row}>
                <td style={styles.fieldInput}>Input Bytes:</td>
                <td style={styles.value}>{FileUtils.getFormattedBytes(jobIOData.get('inputBytes'))}</td>
              </tr>
              <tr className='quote-wrap' style={styles.row}>
                <td style={[styles.inputRecords, styles.fieldInput]}>
                  Input Records:
                </td>
                <td style={styles.value}>{jobsUtils.getFormattedRecords(jobIOData.get('inputRecords'))}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div style={[styles.output]}>
          <h5>Output</h5>
          <table>
            <tbody>
              <tr className='quote-wrap' style={styles.row}>
                <td style={styles.fieldOutput}>Output Bytes:</td>
                <td style={styles.value}>{FileUtils.getFormattedBytes(jobIOData.get('outputBytes'))}</td>
                <td></td>
              </tr>
              <tr className='quote-wrap' style={styles.row}>
                <td style={styles.fieldOutput}>Output Records:</td>
                <td style={styles.value}>{jobsUtils.getFormattedRecords(jobIOData.get('outputRecords'))}</td>
                <td style={styles.truncated}> {jobIOData.get('isOutputLimited') ?
                  <div style={styles.truncatedText}>&nbsp; Automatic Truncation <HoverHelp style={styles.truncatedHover} content={la('UI Jobs are automatically truncated.')}/></div> : ''}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  }
}

export default Quote;

const styles = {
  base: {
    display: 'flex',
    justifyContent: 'flex-start'
  },
  input: {
    minWidth: 155
  },
  fieldOutput: {
    width: 100,
    paddingBottom: 5,
    color: '#999'
  },
  truncated: {
    paddingBottom: 5,
    color: '#999',
    position: 'relative'
  },
  truncatedText: {
    display: 'flex',
    alignItems: 'center'
  },
  truncatedHover: {
    display: 'inline-block',
    position: 'absolute',
    top: -5,
    right: -24,
    color: 'black'
  },
  inputRecords: {
    marginRight: 2
  },
  fieldInput: {
    width: 100,
    paddingBottom: 5,
    color: '#999'
  },
  output: {
    marginLeft: 20
  },
  value: {
    flexGrow: 1,
    textAlign: 'right',
    paddingBottom: 5
  }
};
