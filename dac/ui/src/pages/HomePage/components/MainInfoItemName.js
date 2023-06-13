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
import { Link, location } from "react-router";
import PropTypes from "prop-types";
import Immutable from "immutable";
import FontIcon from "components/Icon/FontIcon";
import DatasetItemLabel from "components/Dataset/DatasetItemLabel";
import EllipsedText from "components/EllipsedText";
import { injectIntl } from "react-intl";
import { splitFullPath } from "utils/pathUtils";
import {
  getIconDataTypeFromEntity,
  getIcebergIconTypeFromEntity,
} from "utils/iconUtils";
import {
  checkIfUserShouldGetDeadLink,
  getHref,
} from "@inject/utils/mainInfoUtils/mainInfoNameUtil";
import { newGetHref } from "@inject/utils/mainInfoUtils/newMainInfoNameUtil";
import { getRefQueryParams } from "@app/utils/nessieUtils";
import { shouldUseNewDatasetNavigation } from "@app/utils/datasetNavigationUtils";

export class MainInfoItemName extends Component {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    entity: PropTypes.object,
    onMount: PropTypes.func, // takes width parameter
    isIceberg: PropTypes.bool,
    showMetadataCard: PropTypes.bool,
    refType: PropTypes.string,
    refValue: PropTypes.string,
  };

  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  constructor(props) {
    super(props);
    this.wrap = null;
  }

  componentDidMount() {
    const { onMount } = this.props;
    if (onMount) {
      onMount(this.getComponentWidth());
    }
  }

  setWrapRef = (element) => {
    this.wrap = element;
  };

  getComponentWidth() {
    return this.wrap.clientWidth;
  }

  renderDatasetItemLabel(shouldGetADeadLink) {
    const { item, isIceberg, showMetadataCard } = this.props;
    const type = item.get("entityType");
    const typeIcon = isIceberg
      ? getIcebergIconTypeFromEntity(item)
      : getIconDataTypeFromEntity(item);
    if (shouldGetADeadLink) {
      return (
        <div style={styles.flexAlign}>
          <FontIcon type={typeIcon} />
          <EllipsedText
            className="--dead-link"
            style={styles.fullPath}
            text={item.get("name")}
          />
        </div>
      );
    } else if (
      type === "dataset" ||
      type === "physicalDataset" ||
      (type === "file" && item.get("queryable")) ||
      (type === "folder" && item.get("queryable"))
    ) {
      return (
        <DatasetItemLabel
          name={item.get("name")}
          item={item}
          showSummaryOverlay={showMetadataCard}
          fullPath={
            item.get("fullPathList") ||
            item.getIn(["fileFormat", "fullPath"]) ||
            splitFullPath(item.get("filePath"))
          }
          typeIcon={typeIcon}
          tooltipPlacement="right"
        />
      );
    }
    return (
      <div style={styles.flexAlign}>
        <FontIcon type={typeIcon} />
        <EllipsedText
          className="last-File"
          style={styles.fullPath}
          text={item.get("name")}
        />
      </div>
    );
  }

  render() {
    const { item, refType, refValue } = this.props;
    const fileType = item.get("fileType");

    let tempHref;
    if (shouldUseNewDatasetNavigation()) {
      tempHref = newGetHref(item, this.context);
    } else {
      tempHref = getHref(item, this.context);
    }

    let href;

    if (typeof tempHref === "string") {
      href =
        tempHref?.includes?.("mode=edit") && refType && refValue
          ? tempHref + `&refType=${refType}&refValue=${refValue}`
          : tempHref;
    } else {
      href = tempHref?.href ? tempHref.href : tempHref;
    }

    const shouldGetADeadLink = checkIfUserShouldGetDeadLink(item);
    const linkStyle =
      fileType === "folder" && !item.get("queryable")
        ? styles.flexAlign
        : { ...styles.flexAlign, ...styles.leafLink };

    return (
      <div
        style={{ ...styles.flexAlign, ...styles.base }}
        className={shouldGetADeadLink ? "--dead-link" : null}
        ref={this.setWrapRef}
      >
        <Link
          style={linkStyle}
          className={shouldGetADeadLink ? "--dead-link" : null}
          to={shouldGetADeadLink ? location : href}
        >
          {this.renderDatasetItemLabel(shouldGetADeadLink)}
        </Link>
      </div>
    );
  }
}

const styles = {
  base: {
    maxWidth: "calc(100% - 100px)", // reserve 100px for tags [IE 11]
  },
  fullPath: {
    marginLeft: 5,
  },
  flexAlign: {
    display: "flex",
    //flex: '0 1', // should get rid ofthis for [IE 11]
    alignItems: "center",
    maxWidth: "100%",
  },
  leafLink: {
    color: "#333",
  },
};

const mapStateToProps = (state, ownProps) => {
  const { isIceberg, item } = ownProps;

  if (!isIceberg) return {};

  const pathList = item?.toJS()?.fullPathList ?? [];
  const sourceName = pathList[0];
  return getRefQueryParams(state.nessie, sourceName);
};

export default injectIntl(connect(mapStateToProps)(MainInfoItemName));
