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
import { Link } from "react-router";
import Immutable from "immutable";
import PropTypes from "prop-types";
import DocumentTitle from "react-document-title";
import { FormattedMessage, injectIntl } from "react-intl";
import urlParse from "url-parse";

import MainInfoMixin from "dyn-load/pages/HomePage/components/MainInfoMixin";
import { loadWiki } from "@app/actions/home";
import DatasetMenu from "components/Menus/HomePage/DatasetMenu";
import FolderMenu from "components/Menus/HomePage/FolderMenu";

import BreadCrumbs, { formatFullPath } from "components/BreadCrumbs";
import { getRootEntityType } from "utils/pathUtils";
import SettingsBtn from "components/Buttons/SettingsBtn";
import FontIcon from "components/Icon/FontIcon";
import { ENTITY_TYPES } from "@app/constants/Constants";
import localStorageUtils from "@app/utils/storageUtils/localStorageUtils";
import Art from "@app/components/Art";
import { TagsAlert } from "@app/pages/HomePage/components/TagsAlert";

import { NESSIE } from "@app/constants/sourceTypes";
import { tableStyles } from "../tableStyles";
import BrowseTable from "./BrowseTable";
import { HeaderButtons } from "./HeaderButtons";
import MainInfoItemNameAndTag from "./MainInfoItemNameAndTag";
import WikiView from "./WikiView";
import SourceBranchPicker from "./SourceBranchPicker/SourceBranchPicker";
import { getSortedSources } from "@app/selectors/home";
import { getSourceByName } from "@app/utils/nessieUtils";

const shortcutBtnTypes = {
  view: "view",
  wiki: "wiki",
  settings: "settings",
};

const getEntityId = (props) => {
  return props && props.entity ? props.entity.get("id") : null;
};

@MainInfoMixin
export class MainInfoView extends Component {
  static contextTypes = {
    location: PropTypes.object.isRequired,
  };

  static propTypes = {
    entity: PropTypes.instanceOf(Immutable.Map),
    entityType: PropTypes.oneOf(Object.values(ENTITY_TYPES)),
    viewState: PropTypes.instanceOf(Immutable.Map),
    updateRightTreeVisibility: PropTypes.func,
    rightTreeVisible: PropTypes.bool,
    isInProgress: PropTypes.bool,
    intl: PropTypes.object.isRequired,
    fetchWiki: PropTypes.func, // (entityId) => Promise
    source: PropTypes.instanceOf(Immutable.Map),
    isSonarSource: PropTypes.bool,
    rootEntityType: PropTypes.string,
  };

  static defaultProps = {
    viewState: Immutable.Map(),
  };

  state = {
    isWikiShown: localStorageUtils.getWikiVisibleState(),
  };

  componentDidMount() {
    this.fetchWiki();
  }

  componentDidUpdate(prevProps) {
    this.fetchWiki(prevProps);
  }

  fetchWiki(prevProps) {
    const oldId = getEntityId(prevProps);
    const newId = getEntityId(this.props);
    if (newId && oldId !== newId) {
      this.props.fetchWiki(newId);
    }
  }

  getActionCell(item) {
    return <ActionWrap>{this.getActionCellButtons(item)}</ActionWrap>;
  }

  getActionCellButtons(item) {
    const dataType = item.get("fileType") || "dataset";
    switch (dataType) {
      case "folder":
        return this.getFolderActionButtons(item);
      case "file":
        return this.getFileActionButtons(item, item.get("permissions"));
      case "dataset":
        return this.getShortcutButtons(item, dataType);
      case "physicalDatasets":
        return this.getShortcutButtons(item, "physicalDataset"); // entities collection uses type name without last 's'
      // looks like 'raw', 'database', and 'table' are legacy entity types that are obsolete.
      default:
        throw new Error("unknown dataType");
    }
  }

  getFolderActionButtons(folder) {
    const isFileSystemFolder = !!folder.get("fileSystemFolder");
    const isQueryAble = folder.get("queryable");
    const permissions = folder.get("permissions")
      ? folder.get("permissions").toJS()
      : null;
    const isAdmin = localStorageUtils.isUserAnAdmin();

    if (isFileSystemFolder && isQueryAble) {
      return this.getShortcutButtons(folder, "folder");
    } else if (
      this.checkToRenderConvertFolderButton(isFileSystemFolder, permissions)
    ) {
      return [
        this.renderConvertButton(folder, {
          icon: (
            <FontIcon
              type="FolderConvert"
              style={{ marginTop: 8, marginLeft: -2 }}
              tooltip={la("Format Folder")}
            />
          ),
          to: {
            ...this.context.location,
            state: {
              modal: "DatasetSettingsModal",
              tab: "format",
              entityType: folder.get("entityType"),
              entityId: folder.get("id"),
              query: { then: "query" },
              isHomePage: true,
            },
          },
        }),
        this.getSettingsBtnByType(<FolderMenu folder={folder} />, folder),
      ];
    } else if (
      isAdmin ||
      (permissions && permissions.canAlter) ||
      permissions.canRead ||
      permissions.canEditAccessControlList ||
      permissions.canDelete
    ) {
      return this.getSettingsBtnByType(<FolderMenu folder={folder} />, folder);
    } else {
      return;
    }
  }

  getFileActionButtons(file, permissions) {
    const isAdmin = localStorageUtils.isUserAnAdmin();
    const isQueryAble = file.get("queryable");
    if (isQueryAble) {
      return this.getShortcutButtons(file, "file");
    }
    // DX-12874 not queryable files should have only promote button
    if (this.checkToRenderConvertFileButton(isAdmin, permissions)) {
      return [
        this.renderConvertButton(file, {
          icon: (
            <FontIcon
              type="FileConvert"
              style={{ marginTop: 8 }}
              tooltip={la("Format File")}
            />
          ),
          to: {
            ...this.context.location,
            state: {
              modal: "DatasetSettingsModal",
              tab: "format",
              entityType: file.get("entityType"),
              entityId: file.get("id"),
              queryable: file.get("queryable"),
              fullPath: file.get("filePath"),
              query: { then: "query" },
              isHomePage: true,
            },
          },
        }),
      ];
    } else {
      return;
    }
  }

  // this method is targeted for dataset like entities: PDS, VDS and queriable files
  getShortcutButtons(item, entityType) {
    const allBtns = this.getShortcutButtonsData(
      item,
      entityType,
      shortcutBtnTypes
    );
    return [
      ...allBtns
        // select buttons to be shown
        .filter((btn) => btn.isShown)
        // return rendered link buttons
        .map((btnType, index) => (
          <Link
            to={btnType.link}
            key={item.get("id") + index}
            className="main-settings-btn min-btn"
            style={{
              marginRight: 5, // all buttons should have 5px margin. Last settings button should not have any margin
            }}
          >
            <button className="settings-button" data-qa={btnType.type}>
              {btnType.label}
            </button>
          </Link>
        )),
      this.getSettingsBtnByType(
        <DatasetMenu entity={item} entityType={entityType} />,
        item
      ),
    ];
  }

  getInlineIcon(iconSrc, alt) {
    return (
      <Art src={iconSrc} alt={alt} title style={{ height: 24, width: 24 }} />
    );
  }

  getWikiButtonLink(item) {
    const url = item.getIn(["links", "query"]);
    const parseUrl = urlParse(url);
    return `${parseUrl.pathname}/wiki${parseUrl.query}`;
  }

  getSettingsBtnByType(menu, item) {
    return (
      <SettingsBtn
        handleSettingsClose={this.handleSettingsClose.bind(this)}
        handleSettingsOpen={this.handleSettingsOpen.bind(this)}
        dataQa={item.get("name")}
        menu={menu}
        key={`${item.get("name")}-${item.get("id")}`}
        hideArrowIcon
      >
        {this.getInlineIcon("Ellipsis.svg", "more")}
      </SettingsBtn>
    );
  }

  getRow(item) {
    const [name, jobs, action] = this.getTableColumns();
    const jobsCount =
      item.get("jobCount") ||
      item.getIn(["extendedConfig", "jobCount"]) ||
      la("—");
    return {
      rowClassName: item.get("name"),
      data: {
        [name.key]: {
          node: () => <MainInfoItemNameAndTag item={item} />,
          value: item.get("name"),
        },
        [jobs.key]: {
          node: () => (
            <Link to={item.getIn(["links", "jobs"])}>{jobsCount}</Link>
          ),
          value: jobsCount,
        },
        [action.key]: {
          node: () => this.getActionCell(item),
        },
      },
    };
  }

  getTableColumns() {
    const { intl } = this.props;
    return [
      {
        key: "name",
        label: intl.formatMessage({ id: "Common.Name" }),
        infoContent: <TagsAlert />,
        flexGrow: 1,
      },
      {
        key: "jobs",
        label: intl.formatMessage({ id: "Job.Jobs" }),
        style: tableStyles.digitColumn,
        headerStyle: { justifyContent: "flex-end" },
        isFixedWidth: true,
        width: 60,
      },
      {
        key: "action",
        label: intl.formatMessage({ id: "Common.Action" }),
        style: tableStyles.actionColumn,
        isFixedWidth: true,
        width: 140,
        className: "row-buttons",
        headerClassName: "row-buttons",
        disableSort: true,
      },
    ];
  }

  getTableData() {
    const contents = this.props.entity && this.props.entity.get("contents");
    let rows = Immutable.List();
    if (contents && !contents.isEmpty()) {
      const appendRow = (dataset) => {
        // DX-10700 there could be the cases, when we reset a entity cache (delete all entities of the certain type), but there are references on these intities in the state;
        // For example, we clearing folders, when we navigating to another folder, but sources could have a reference by id on these folder. As folder would not be found, here we would have undefined
        //skip such cases
        if (!dataset) return;
        rows = rows.push(this.getRow(dataset));
      };
      contents.get("datasets").forEach(appendRow);
      contents.get("folders").forEach(appendRow);
      contents.get("files").forEach(appendRow);
      contents.get("physicalDatasets").forEach(appendRow);
    }
    return rows;
  }

  handleSettingsClose(settingsWrap) {
    $(settingsWrap).parents("tr").removeClass("hovered");
  }

  handleSettingsOpen(settingsWrap) {
    $(settingsWrap).parents("tr").addClass("hovered");
  }

  isReadonly(spaceList) {
    const { pathname } = this.context.location;
    if (spaceList !== undefined) {
      return !!spaceList.find((item) => {
        if (item.href === pathname) {
          return item.readonly;
        }
      });
    }
    return false;
  }

  toggleWikiShow = () => {
    let newValue;
    this.setState(
      (prevState) => ({
        isWikiShown: (newValue = !prevState.isWikiShown),
      }),
      () => {
        localStorageUtils.setWikiVisibleState(newValue);
      }
    );
  };

  toggleRightTree = () => {
    this.props.updateRightTreeVisibility(!this.props.rightTreeVisible);
  };

  isNessie = () => {
    const { source, entity } = this.props;
    return entity && source && source.get("type") === NESSIE;
  };

  renderExternalLink = () => {
    const { source } = this.props;
    if (!this.isNessie()) return null;
    return (
      <Link to={`/sources/dataplane/${source.get("name")}/branches`}>
        <FormattedMessage id="Nessie.ViewAllBranches" />
      </Link>
    );
  };

  renderTitleExtraContent = () => {
    const { source } = this.props;
    if (!this.isNessie()) return null;
    return <SourceBranchPicker source={source.toJS()} />;
  };

  render() {
    const { entity, viewState, isSonarSource, rootEntityType } = this.props;
    const { pathname } = this.context.location;
    const showWiki = entity && !entity.get("fileSystemFolder"); // should be removed when DX-13804 would be fixed

    const buttons = entity && (
      <HeaderButtons
        entity={entity}
        rootEntityType={rootEntityType}
        rightTreeVisible={this.props.rightTreeVisible}
        toggleVisibility={this.toggleRightTree}
        isSonarSource={isSonarSource}
      />
    );

    return (
      <BrowseTable
        title={
          entity && (
            <BreadCrumbs
              fullPath={entity.get("fullPathList")}
              pathname={pathname}
              showCopyButton
            />
          )
        }
        buttons={buttons}
        key={pathname} /* trick to clear out the searchbox on navigation */
        columns={this.getTableColumns()}
        rightSidebar={showWiki ? <WikiView item={entity} /> : null}
        rightSidebarExpanded={this.state.isWikiShown}
        toggleSidebar={this.toggleWikiShow}
        tableData={this.getTableData()}
        viewState={viewState}
        renderExternalLink={this.renderExternalLink}
        renderTitleExtraContent={this.renderTitleExtraContent}
      >
        <DocumentTitle
          title={
            (entity && formatFullPath(entity.get("fullPathList")).join(".")) ||
            ""
          }
        />
      </BrowseTable>
    );
  }
}
MainInfoView = injectIntl(MainInfoView);

export const styles = {
  height: {
    height: "100%",
  },
  loader: {
    display: "flex",
    justifyContent: "center",
    color: "gray",
    marginTop: 10,
    fontSize: 22,
  },
  viewerHeader: {
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
  },
  folderConvertButton: {
    borderRadius: "2px",
    height: 23,
    width: 68,
    boxShadow: "0 1px 1px #b2bec7",
    cursor: "pointer",
    display: "flex",
    paddingTop: 1,
    marginRight: 5,
  },
  button: {
    borderRadius: "2px",
    height: 23,
    width: 68,
    boxShadow: "0 1px 1px #b2bec7",
    cursor: "pointer",
    display: "flex",
    paddingTop: 1,
  },
  searchField: {
    width: 200,
    height: 30,
  },
};

function ActionWrap({ children }) {
  return <span className="action-wrap">{children}</span>;
}
ActionWrap.propTypes = {
  children: PropTypes.node,
};

function mapStateToProps(state, props) {
  const rootEntityType = getRootEntityType(
    props.entity?.getIn(["links", "self"])
  );

  let isSonarSource = false;
  const sources = getSortedSources(state);
  if (
    sources &&
    rootEntityType === ENTITY_TYPES.source &&
    props.entity?.get("entityType") === ENTITY_TYPES.folder
  ) {
    const entityJS = props.entity.toJS();
    const parentSource = getSourceByName(
      entityJS.fullPathList[0],
      sources.toJS()
    );

    isSonarSource = parentSource?.type === NESSIE;
  }

  return {
    rootEntityType,
    isSonarSource,
  };
}

export default connect(mapStateToProps, (dispatch) => ({
  fetchWiki: loadWiki(dispatch),
}))(MainInfoView);
