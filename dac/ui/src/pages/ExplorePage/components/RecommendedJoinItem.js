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
import PropTypes from "prop-types";
import Immutable from "immutable";
import classNames from "classnames";
import FontIcon from "components/Icon/FontIcon";
import DatasetItemLabel from "components/Dataset/DatasetItemLabel";

import * as classes from "./RecommendedJoinItem.module.less";

export class RecommendedJoinItem extends Component {
  static propTypes = {
    recommendation: PropTypes.instanceOf(Immutable.Map),
    onClick: PropTypes.func,
    isActive: PropTypes.bool,
    selectJoin: PropTypes.func,
  };

  static defaulProps = {
    isActive: false,
  };

  getRecommendedDatasetName() {
    return this.props.recommendation.getIn(["rightTableFullPathList", -1]);
  }

  goToCustomTab = (e) => {
    this.props.selectJoin(this.props.recommendation, true);
    e.stopPropagation();
  };

  selectRecommendation = (e) => {
    this.props.selectJoin(this.props.recommendation, false);
    e.stopPropagation();
  };

  renderJoinType() {
    const { recommendation } = this.props;
    const joinTypeToIconType = {
      Inner: "JoinInner",
      LeftOuter: "JoinLeft",
      RightOuter: "JoinRight",
      FullOuter: "JoinFull",
    };
    return (
      <FontIcon
        dataQa="joinType"
        type={joinTypeToIconType[recommendation.get("joinType")]}
      />
    );
  }

  renderEditButton() {
    return (
      <div
        className={classNames(classes["edit-button"], {
          [classes["edit-button--active"]]: !!this.props.isActive,
        })}
        onClick={this.goToCustomTab}
      >
        <FontIcon type="Edit" />
      </div>
    );
  }

  renderMatchingKeys() {
    const currentKeys = this.props.recommendation.get("matchingKeys");
    const matchingKeys = currentKeys.keySeq();

    return matchingKeys.map((matchingKey, i) => (
      <div key={i} style={{ display: "flex", width: "100%" }}>
        <div data-qa="Current Dataset Key" style={{ width: "100%" }}>
          {matchingKey}
        </div>
        <div style={{ width: "100%" }}>=</div>
        <div data-qa="Matching Key" style={{ width: "100%" }}>
          {currentKeys.get(matchingKey)}
        </div>
      </div>
    ));
  }

  render() {
    const fullPathList = this.props.recommendation.get(
      "rightTableFullPathList"
    );
    return (
      <div
        className={classNames(classes["recommended-join-item"], {
          [classes["recommended-join-item--active"]]: !!this.props.isActive,
        })}
        key="base"
        onClick={this.selectRecommendation}
      >
        <div style={styles.name}>
          <DatasetItemLabel
            name={this.getRecommendedDatasetName()}
            fullPath={fullPathList}
            placement="right"
            showFullPath
            typeIcon="VirtualDataset" // TODO get dataset type from server. DX-5884
          />
        </div>
        <div style={styles.type}>{this.renderJoinType()}</div>
        <div style={styles.cur}>
          <div style={styles.keys}>{this.renderMatchingKeys()}</div>
          {this.renderEditButton()}
        </div>
      </div>
    );
  }
}

const styles = {
  name: {
    display: "flex",
    alignItems: "center",
    width: "50%",
    minWidth: 300,
  },
  type: {
    display: "flex",
    alignItems: "center",
    width: "50%",
    minWidth: 100,
  },
  cur: {
    display: "flex",
    alignItems: "center",
    width: "100%",
    minWidth: 400,
  },
  keys: {
    display: "flex",
    flexDirection: "column",
    minWidth: 400,
    width: "85%",
  },
};
export default RecommendedJoinItem;
