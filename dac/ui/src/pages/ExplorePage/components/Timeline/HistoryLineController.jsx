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
import Immutable from "immutable";

import { getHistoryItems } from "selectors/explore";
import exploreUtils from "#oss/utils/explore/exploreUtils";
import { memoOne } from "#oss/utils/memoUtils";
import { cloneDeep } from "lodash";
import { pageTypesProp } from "../../pageTypes";

import HistoryLine from "./HistoryLine";

const getAlteredHistoryItemsMemo = memoOne((items) => {
  const historyItems = cloneDeep(items);
  const jsHistoryItems = historyItems.toJS();
  jsHistoryItems.pop();

  return Immutable.List(jsHistoryItems).map((item) => Immutable.Map(item));
});

export class HistoryLineController extends PureComponent {
  static propTypes = {
    dataset: PropTypes.instanceOf(Immutable.Map).isRequired,
    historyItems: PropTypes.instanceOf(Immutable.List),
    location: PropTypes.object.isRequired,
    pageType: pageTypesProp,
  };

  constructor(props) {
    super(props);
  }

  render() {
    const { dataset, historyItems, location, pageType } = this.props;

    return (
      <HistoryLine
        location={location}
        historyItems={historyItems}
        tipVersion={dataset.get("tipVersion")}
        activeVersion={dataset.get("datasetVersion")}
        pageType={pageType}
      />
    );
  }
}

function mapStateToProps(state, ownProps) {
  const version = ownProps.dataset.get("tipVersion");
  const entities = state.resources.entities;
  const history = entities.get("history");
  const historyItem = entities.get("historyItem");
  const datasetUI = entities.get("datasetUI");
  const [, isInPhysicalHistory] = exploreUtils.getIfInEntityHistory(
    history,
    historyItem,
    datasetUI,
    version,
  );

  let historyItems = getHistoryItems(state, version);
  if (isInPhysicalHistory) {
    historyItems = getAlteredHistoryItemsMemo(historyItems);
  }

  return {
    historyItems,
  };
}

export default connect(mapStateToProps)(HistoryLineController);
