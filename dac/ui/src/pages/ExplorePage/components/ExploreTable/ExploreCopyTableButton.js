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
import { PureComponent } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { addNotification } from "@app/actions/notification";
import CopyButtonIcon from "@app/components/Buttons/CopyButtonIcon";
import { getPaginationJobId } from "@app/selectors/explore";
import { escapeDblQuotes } from "@app/utils/regExpUtils";
import ApiUtils from "@app/utils/apiUtils/apiUtils";
import { copyTextToClipboard } from "@app/utils/clipboard/clipboardUtils";
import { MSG_CLEAR_DELAY_SEC } from "@app/constants/Constants";

const MAX_ROWS_TO_CLIPBOARD = 5000;

export class ExploreCopyTableButton extends PureComponent {
  static propTypes = {
    title: PropTypes.string,
    version: PropTypes.string,
    // connected
    addNotification: PropTypes.func.isRequired,
    jobId: PropTypes.string,
  };

  static defaultProps = {
    title: "Copy table content to clipboard",
  };

  state = {
    isPreparing: false,
  };

  textToCopy = "";
  isMaxReached = false;

  componentDidUpdate(prevProps) {
    if (prevProps.jobId !== this.props.jobId) {
      this.textToCopy = "";
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
    if (typeof val === "string" && val.includes("\t")) {
      return `"${escapeDblQuotes(val)}"`;
    }
    return val;
  };

  static makeCopyTextFromTableData = (tableData) => {
    // make data array with copied elements
    const dataArray = tableData.rows.map((rowEntry) => {
      return rowEntry.row.map((el) =>
        ExploreCopyTableButton.prepareValueForTabDelimitedItem(el.v)
      );
    });
    // since we loaded MAX_ROWS_TO_CLIPBOARD + 1 to detect if we reached max, remove last row
    if (dataArray.length > MAX_ROWS_TO_CLIPBOARD) {
      dataArray.pop();
    }
    // prepend data rows with the row of column names
    dataArray.unshift(
      tableData.columns.map((col) =>
        ExploreCopyTableButton.prepareValueForTabDelimitedItem(col.name)
      )
    );
    // make text string for copy: items are tab-delimited, rows end with LF/CR
    const rowArray = dataArray.map((row) => row.join("\t"));
    return rowArray.join("\r\n");
  };

  copyText = () => {
    const success = copyTextToClipboard(this.textToCopy);
    this.setState({ isPreparing: false });
    if (success) {
      const message = this.isMaxReached
        ? la(
            `The first ${MAX_ROWS_TO_CLIPBOARD.toLocaleString()} were copied to the clipboard. Use download if you want to extract the entire result set.`
          )
        : la("Table data is copied to clipboard.");
      this.props.addNotification(message, "success", MSG_CLEAR_DELAY_SEC);
    } else {
      this.props.addNotification(
        la("Failed to copy to clipboard. Please use download feature."),
        "warning",
        MSG_CLEAR_DELAY_SEC
      );
    }
  };

  handleClick = () => {
    if (this.textToCopy) {
      //text already prepared - this is duplicate click for the same jobId
      this.setState({ isPreparing: true });
      this.timeoutHandle = setTimeout(this.copyText, 1);
      return;
    }

    // fetch full result set for jobId
    const { jobId } = this.props;
    if (!jobId) {
      this.props.addNotification(
        la("Missing job id to fetch data for clipboard."),
        "error",
        MSG_CLEAR_DELAY_SEC
      );
      return;
    }
    const url = `job/${jobId}/data?offset=0&limit=${MAX_ROWS_TO_CLIPBOARD + 1}`;
    const options = { headers: ApiUtils.getJobDataNumbersAsStringsHeader() };
    this.setState({ isPreparing: true });
    ApiUtils.fetchJson(
      url,
      (json) => {
        // get tableData from response, make textToCopy, copy, setState
        this.textToCopy =
          ExploreCopyTableButton.makeCopyTextFromTableData(json);
        this.isMaxReached = json.returnedRowCount > MAX_ROWS_TO_CLIPBOARD;
        this.copyText();
      },
      (error) => {
        // handle error for both api and json parse
        const msg = la("Error fetching data for clipboard.");
        this.props.addNotification(
          `${msg}: ${error.errorMessage}`,
          "error",
          MSG_CLEAR_DELAY_SEC
        );
        console.error(msg);
        this.setState({ isPreparing: false });
      },
      options,
      2
    );
  };

  render() {
    const { title, jobId } = this.props;
    const isDisabled = !jobId;

    return (
      <CopyButtonIcon
        title={title}
        onClick={this.handleClick}
        disabled={isDisabled}
        isLoading={this.state.isPreparing}
      />
    );
  }
}

function mapStateToProps(state, props) {
  const { version } = props;
  const jobId = (version && getPaginationJobId(state, version)) || "";

  return {
    jobId,
  };
}

export default connect(mapStateToProps, {
  addNotification,
})(ExploreCopyTableButton);
