/*
 * Copyright (C) 2017-2019 Dremio Corporation
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
import { PureComponent } from 'react';
import { connect } from 'react-redux';
import PropTypes from 'prop-types';
import platform from 'platform';
import { addNotification } from '@app/actions/notification';
import CopyButtonIcon from '@app/components/Buttons/CopyButtonIcon';
import { getPaginationJobId } from '@app/selectors/explore';
import { escapeDblQuotes } from '@app/utils/regExpUtils';
import ApiUtils from '@app/utils/apiUtils/apiUtils';

const MAX_ROWS_TO_CLIPBOARD = 5000;
const MSG_CLEAR_DELAY_SEC = 3;
const isFirefox = platform.name === 'Firefox';
const isSafari = platform.name === 'Safari';
const isEdge = platform.name === 'Microsoft Edge';

// Copies a string to the clipboard. Must be called from within an event handler such as click.
// May return false if it failed, but this is not always
// possible. Browser support for Chrome 43+, Firefox 42+, Edge and IE 10+.
// No Safari support, as of (Nov. 2015). Returns false.
// IE: The clipboard feature may be disabled by an adminstrator. By default a prompt is
// shown the first time the clipboard is used (per session).
// Inspired by: Greg Lowe https://jsfiddle.net/fx6a6n6x/
function copyTextToClipboard(text) {
  if (window.clipboardData && window.clipboardData.setData) {
    // IE specific code path to prevent textarea being shown while dialog is visible.
    return window.clipboardData.setData('Text', text);
  }
  if (document.queryCommandSupported && document.queryCommandSupported('copy')) {
    let success = false;
    const textarea = document.createElement('textarea');
    textarea.textContent = text;
    textarea.style.position = 'fixed';  // Prevent scrolling to bottom of page in MS Edge.
    textarea.style.zIndex = '-1';
    document.body.appendChild(textarea);
    textarea.select();
    try {
      success = document.execCommand('copy');  // Security exception may be thrown by some browsers.
    } catch (ex) {
      console.warn('Copy to clipboard failed.', ex);
    } finally {
      document.body.removeChild(textarea);
    }
    return success;
  }
  return false;
}


export class ExploreCopyTableButton extends PureComponent {
  static propTypes = {
    title: PropTypes.string,
    style: PropTypes.object,
    version: PropTypes.string,
    // connected
    addNotification: PropTypes.func.isRequired,
    jobId: PropTypes.string
  };

  static defaultProps = {
    title: la('Copy table content to clipboard')
  };

  state = {
    isPreparing: false
  };

  textToCopy = '';
  isMaxReached = false;

  componentDidUpdate(prevProps) {
    if (prevProps.jobId !== this.props.jobId) {
      this.textToCopy = '';
      this.isMaxReached = false;
    }
  }

  componentWillUnmount() {
    if (this.timeoutHandle) {
      clearTimeout(this.timeoutHandle);
      this.timeoutHandle = 0;
    }
  }

  static prepareValueForTabDelimitedItem = (val) => {
    if (val instanceof Object) {
      return JSON.stringify(val);
    }
    // In case value has tab(s), wrap it in dbl-quotes and escape internal dbl-quotes
    if (typeof val === 'string' && val.includes('\t')) {
      return `"${escapeDblQuotes(val)}"`;
    }
    return val;
  };

  static makeCopyTextFromTableData = (tableData) => {
    // make data array with copied elements
    const dataArray = tableData.rows.map(rowEntry => {
      return rowEntry.row.map(el => ExploreCopyTableButton.prepareValueForTabDelimitedItem(el.v));
    });
    // since we loaded MAX_ROWS_TO_CLIPBOARD + 1 to detect if we reached max, remove last row
    if (dataArray.length > MAX_ROWS_TO_CLIPBOARD) {
      dataArray.pop();
    }
    // prepend data rows with the row of column names
    dataArray.unshift(tableData.columns.map(col => ExploreCopyTableButton.prepareValueForTabDelimitedItem(col.name)));
    // make text string for copy: items are tab-delimited, rows end with LF/CR
    const rowArray = dataArray.map(row => row.join('\t'));
    return rowArray.join('\r\n');
  };

  copyText = () => {
    const success = copyTextToClipboard(this.textToCopy);
    this.setState({isPreparing: false});
    if (success) {
      const message = (this.isMaxReached) ?
        la(`The first ${MAX_ROWS_TO_CLIPBOARD.toLocaleString()} were copied to the clipboard. Use download if you want to extract the entire result set.`) :
        la('Table data is copied to clipboard.');
      this.props.addNotification( message, 'success', MSG_CLEAR_DELAY_SEC);
    } else {
      this.props.addNotification( la('Failed to copy to clipboard. Please use download feature.'), 'warning', MSG_CLEAR_DELAY_SEC);
    }
  };

  handleClick = () => {
    if (this.textToCopy) { //text already prepared - this is duplicate click for the same jobId
      this.setState({isPreparing: true});
      if (isFirefox) {
        // firefox requires clipboard command in click event direct handler
        this.copyText();
      } else {
        // use setTimeout to allow spinner to be shown first
        this.timeoutHandle = setTimeout(this.copyText, 1);
      }
      return;
    }

    // fetch full result set for jobId
    const { jobId  } = this.props;
    if (!jobId) {
      this.props.addNotification(la('Missing job id to fetch data for clipboard.'), 'error', MSG_CLEAR_DELAY_SEC);
      return;
    }
    const url = `job/${jobId}/data?offset=0&limit=${MAX_ROWS_TO_CLIPBOARD + 1}`;
    const options = {headers: ApiUtils.getJobDataNumbersAsStringsHeader()};
    this.setState({isPreparing: true});
    ApiUtils.fetch(url, options, 2).then(response => {
      // get tableData from response, make textToCopy, copy, setState
      return response.json().then(data => {
        this.textToCopy = ExploreCopyTableButton.makeCopyTextFromTableData(data);
        this.isMaxReached = data.returnedRowCount > MAX_ROWS_TO_CLIPBOARD;
        if (isFirefox || isSafari) {
          // firefox and safari do not allow copy to clipboard here
          this.props.addNotification(la('Due to browser security settings please click copy icon again.'), 'info', MSG_CLEAR_DELAY_SEC);
          this.setState({isPreparing: false});
        } else {
          this.copyText();
        }
      });
    }, error => {
      this.props.addNotification(`${la('Error fetching data for clipboard')}: ${error.errorMessage}`, 'error', MSG_CLEAR_DELAY_SEC);
      console.error('Error fetching data for clipboard');
      this.setState({isPreparing: false});
    });

  };

  render() {
    if (isEdge) { // can't copy to clipboard in MS Edge using current technique.
      return null;
    }

    const { title, style, jobId } = this.props;
    const isDisabled = !jobId;

    return (
      <CopyButtonIcon
        title={title}
        style={style}
        onClick={this.handleClick}
        disabled={isDisabled}
        showSpinner={this.state.isPreparing}
      />
    );
  }

}

function mapStateToProps(state, props) {
  const { version } = props;
  const jobId = version && getPaginationJobId(state, version) || '';

  return {
    jobId
  };
}

export default connect(mapStateToProps, {
  addNotification
})(ExploreCopyTableButton);

