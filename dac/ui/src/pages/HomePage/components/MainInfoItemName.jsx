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
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { shouldUseNewDatasetNavigation } from "#oss/utils/datasetNavigationUtils";
import { Popover } from "dremio-ui-lib/components";
import EntitySummaryOverlay from "#oss/components/EntitySummaryOverlay/EntitySummaryOverlay";

export class MainInfoItemName extends Component {
  static propTypes = {
    item: PropTypes.instanceOf(Immutable.Map).isRequired,
    intl: PropTypes.object.isRequired,
    entity: PropTypes.object,
    onMount: PropTypes.func, // takes width parameter
    versionContext: PropTypes.object,
    openDetailsPanel: PropTypes.func,
    tagsLength: PropTypes.number,
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
    const { item, versionContext, openDetailsPanel } = this.props;
    const type = item.get("entityType");
    const typeIcon = versionContext
      ? getIcebergIconTypeFromEntity(item)
      : getIconDataTypeFromEntity(item); //
    const defaultItem = (
      <div style={styles.flexAlign} className="dremio-icon-label">
        <dremio-icon name={typeIcon} />
        <EllipsedText className="last-File" text={item.get("name")} />
      </div>
    );
    if (shouldGetADeadLink) {
      return (
        <div style={styles.flexAlign} className="dremio-icon-label">
          <dremio-icon name={typeIcon} />
          <EllipsedText className="--dead-link" text={item.get("name")} />
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
          fullPath={
            item.get("fullPathList") ||
            item.getIn(["fileFormat", "fullPath"]) ||
            splitFullPath(item.get("filePath"))
          }
          typeIcon={typeIcon}
          versionContext={versionContext}
          tooltipPlacement="right"
          openDetailsPanel={openDetailsPanel}
        />
      );
    } else if (type === "folder") {
      return (
        <Popover
          role="tooltip"
          showArrow
          delay={750}
          placement="right"
          mode="hover"
          portal
          content={
            <EntitySummaryOverlay
              name={item.get("name")}
              type={"FOLDER"}
              fullPath={item.get("fullPathList")}
              entityUrl={item.getIn(["links", "self"])}
              versionContext={versionContext}
              openDetailsPanel={openDetailsPanel}
            />
          }
        >
          {defaultItem}
        </Popover>
      );
    }
    return defaultItem;
  }

  render() {
    const { item, versionContext, tagsLength } = this.props;

    let tempHref;
    if (shouldUseNewDatasetNavigation()) {
      tempHref = newGetHref(item, this.context);
    } else {
      tempHref = getHref(item, this.context);
    }

    let href;

    if (typeof tempHref === "string") {
      const { type, value } = versionContext ?? {};
      href =
        tempHref?.includes?.("mode=edit") && type && value
          ? tempHref + `&refType=${type}&refValue=${value}`
          : tempHref;
    } else {
      href = tempHref?.href ? tempHref.href : tempHref;
    }

    const shouldGetADeadLink = checkIfUserShouldGetDeadLink(item);

    return (
      <div
        style={{
          ...styles.flexAlign,
          maxWidth: `calc(100% - ${tagsLength > 0 ? "160" : "32"}px)`,
        }}
        className={shouldGetADeadLink ? "--dead-link" : null}
        ref={this.setWrapRef}
      >
        <Link
          style={{ ...styles.flexAlign, ...styles.bold }}
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
    maxWidth: "calc(100% - 160px)", // reserve 100px for tags [IE 11]
  },
  flexAlign: {
    display: "flex",
    //flex: '0 1', // should get rid ofthis for [IE 11]
    alignItems: "center",
    maxWidth: "100%",
  },
  leafLink: {
    color: "var(--text--primary)",
  },
  bold: {
    fontWeight: 500,
  },
};

const mapStateToProps = (state, ownProps) => {
  const { item } = ownProps;

  const versionContext = getVersionContextFromId(item.get("id"));

  return {
    versionContext,
  };
};

export default injectIntl(connect(mapStateToProps)(MainInfoItemName));
