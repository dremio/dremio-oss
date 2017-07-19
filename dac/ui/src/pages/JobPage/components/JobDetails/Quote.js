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
import Immutable  from 'immutable';
import pureRender from 'pure-render-decorator';
import Radium from 'radium';

import { h4 } from 'uiTheme/radium/typography';
import jobsUtils from 'utils/jobsUtils';
import FileUtils from 'utils/FileUtils';

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
          <div className='title' style={[h4, styles.header]}>Input</div>
          <table>
            <tbody>
              <tr className='quote-wrap' style={styles.row}>
                <td className='sub-title' style={styles.fieldInput}>Input Bytes:</td>
                <td style={styles.value}>{FileUtils.getFormattedBytes(jobIOData.get('inputBytes'))}</td>
              </tr>
              <tr className='quote-wrap' style={styles.row}>
                <td className='sub-title' style={[styles.inputRecords, styles.fieldInput]}>
                  Input Records:
                </td>
                <td style={styles.value}>{jobsUtils.getFormattedRecords(jobIOData.get('inputRecords'))}</td>
              </tr>
            </tbody>
          </table>
        </div>
        <div style={[styles.output]}>
          <div className='title' style={[h4, styles.header]}>Output</div>
          <table>
            <tbody>
              <tr className='quote-wrap' style={styles.row}>
                <td className='sub-title' style={styles.fieldOutput}>Output Bytes:</td>
                <td style={styles.value}>{FileUtils.getFormattedBytes(jobIOData.get('outputBytes'))}</td>
              </tr>
              <tr className='quote-wrap' style={styles.row}>
                <td className='sub-title' style={styles.fieldOutput}>Output Records:</td>
                <td style={styles.value}>{jobsUtils.getFormattedRecords(jobIOData.get('outputRecords'))}</td>
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
  header: {
    marginBottom: 15,
    marginTop: 10
  },
  fieldOutput: {
    width: 100,
    paddingBottom: 5
  },
  inputRecords: {
    marginRight: 2
  },
  fieldInput: {
    width: 100,
    paddingBottom: 5
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
