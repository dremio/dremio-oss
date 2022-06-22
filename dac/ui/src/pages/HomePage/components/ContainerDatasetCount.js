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
import { Component } from "react";
import { connect } from "react-redux";
import PropTypes from "prop-types";
import { Tooltip } from "dremio-ui-lib";
import { getDatasetCountStats } from "@app/selectors/home";

const mapStateToProps = (state, { entityId }) => ({
  ...getDatasetCountStats(state, entityId),
});

export class ContainerDatasetCount extends Component {
  static propTypes = {
    count: PropTypes.number,
    isBounded: PropTypes.bool,
  };

  shouldComponentUpdate(nextProps) {
    const { count } = this.props;
    if (count !== nextProps.count) {
      if (count !== undefined && nextProps.count === undefined) {
        return false;
      } else {
        return true;
      }
    } else {
      return false;
    }
  }

  render() {
    const { count, isBounded } = this.props;
    let displayedValue = null;

    if (count !== undefined) {
      if (isBounded) {
        if (count === 0) {
          // we found nothing and were count/time bound, so display '-'
          displayedValue = la("-");
        } else {
          // we found some datasets and were count/time bound, so add '+' to the dataset number
          displayedValue = count + la("+");
        }
      } else {
        displayedValue = count;
      }
    }

    return (
      displayedValue !== null && (
        <Tooltip title="LeftPanel.DatasetCount">
          <span className="count">{displayedValue}</span>
        </Tooltip>
      )
    );
  }
}

export default connect(mapStateToProps)(ContainerDatasetCount);
