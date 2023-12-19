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
import Immutable from "immutable";
import PropTypes from "prop-types";
import { injectIntl } from "react-intl";
import { createSelector } from "reselect";
import { OrderedMap, fromJS } from "immutable";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import { isNotSoftware } from "dyn-load/utils/versionUtils";
import { withRouteLeaveSubscription } from "@app/containers/RouteLeave.js";
import MarkdownEditor from "@app/components/MarkdownEditor";
import { toolbarHeight as toolbarHeightCssValue } from "@app/components/MarkdownEditor.less";
import { startSearch as startSearchAction } from "actions/search";
import ApiUtils from "@app/utils/apiUtils/apiUtils";
import { showConfirmationDialog } from "@app/actions/confirmation";
import { addNotification } from "@app/actions/notification";
import WikiEmptyState from "@app/components/WikiEmptyState";
import { IconButton } from "dremio-ui-lib";
import WikiWrapper from "./WikiModal/WikiWrapper";
import ShrinkableSearch from "./ShrinkableSearch/ShrinkableSearch";
import { getVersionContextFromId } from "dremio-ui-common/utilities/datasetReference.js";
import { hideForNonDefaultBranch } from "dremio-ui-common/utilities/versionContext.js";
import { collapsibleToolbarIcon, collapseButton } from "./Wiki.less";

const toolbarHeight = parseInt(toolbarHeightCssValue, 10);

const getTags = createSelector(
  (state) => state,
  (state) => state.toList()
);
const tagKeyGetter = (tag) => tag.toUpperCase().trim();
const defaultTagsState = {
  tags: new OrderedMap(), // tags for edit mode. Immutable object created by fromJS function
  tagsVersion: null,
};
const getLoadViewState = createSelector(
  (showLoadMask) => showLoadMask,
  (showLoadMask) =>
    fromJS({
      isInProgress: showLoadMask,
    })
);

const mapStateToProps = () => {
  return {
    showLoadMask: false,
  };
};

export class WikiView extends PureComponent {
  static propTypes = {
    entityId: PropTypes.string, // an id of space/dataset/etc
    className: PropTypes.string,
    addHasChangesHook: PropTypes.func.isRequired, // (hasChangesHook: () => true) => void
    startSearch: PropTypes.func, // (textToSearch) => {}
    isEditAllowed: PropTypes.bool, // indicates weather or not a user has manage tags/wiki permissions
    showConfirmationDialog: PropTypes.func,
    showLoadMask: PropTypes.bool,
    addNotification: PropTypes.func,
    dataset: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    overlay: PropTypes.bool,
    isPanel: PropTypes.bool,
    hideSqlEditorIcon: PropTypes.bool,
    hideGoToButton: PropTypes.bool,
    handlePanelDetails: PropTypes.func,
    isLoadingDetails: PropTypes.bool,
  };

  // state and related properties ----------------------------
  state = {
    isWikiInEditMode: false,
    wikiSummary: false,
    wikiViewState: fromJS({}),
    wiki: "",
    wikiVersion: null,
    sidebarCollapsed: false,

    // Tags
    ...defaultTagsState,
    tagsViewState: fromJS({}),

    // Fields
    fields: fromJS([]),
    searchTerm: "",
    apiError: null,
  };

  wikiChanged = false;
  isTagsInEditMode = false; // save request in a process
  // ----------------------------------------------------------

  hasChanges = () => {
    const { isWikiInEditMode } = this.state;

    return (isWikiInEditMode && this.wikiChanged) || this.isTagsInEditMode;
  };

  initEntity = (entityId) => {
    //reset state
    this.setState({
      wikiViewState: fromJS({
        isInProgress: true,
      }),
      ...defaultTagsState,
      tagsViewState: fromJS({
        isInProgress: true,
      }),
      apiError: null,
    });

    const { intl } = this.props;
    const versionContext = getVersionContextFromId(entityId);
    const shouldFetchTagsAndWiki = hideForNonDefaultBranch(versionContext);

    // load tags and wiki for entities that are either not versioned,
    // or belong to the default branch
    if (shouldFetchTagsAndWiki) {
      ApiUtils.fetchJson(
        `catalog/${entityId}/collaboration/tag`,
        this.setOriginalTags,
        (error) => {
          this.setState({ apiError: true });
          if (this.isError(error)) {
            error.json().then((e) => {
              if (e.errorMessage) {
                this.setState({
                  tagsViewState: this.getErrorViewState(e.errorMessage),
                });
              } else {
                this.setState({
                  tagsViewState: this.getErrorViewState(
                    intl.formatMessage({ id: "Wiki.FailedLabels" })
                  ),
                });
              }
            });
          } else {
            // init state with default value
            this.setOriginalTags();
          }
        }
      );

      ApiUtils.fetchJson(
        `catalog/${entityId}/collaboration/wiki`,
        this.setWiki,
        (error) => {
          this.setState({ apiError: true });
          if (this.isError(error)) {
            error.json().then((e) => {
              if (e.errorMessage) {
                this.setState({
                  wikiViewState: this.getErrorViewState(e.errorMessage),
                });
              } else {
                this.setState({
                  wikiViewState: this.getErrorViewState(
                    intl.formatMessage({ id: "Wiki.FailedWiki" })
                  ),
                });
              }
            });
          } else {
            // init state with default value
            this.setWiki();
          }
        }
      );
    } else {
      this.setOriginalTags();
      this.setWiki();
    }
  };

  initFields = (dataset) => {
    ApiUtils.fetchJson(
      dataset.getIn(["apiLinks", "datagraph"]),
      this.setFields,
      () => {},
      {},
      2
    );
  };

  initFieldsOverlay = (dataset) => {
    if ((this.props.overlay || this.props.isPanel) && dataset.get("fields")) {
      this.setState({
        fields: dataset.get("fields"),
        origFields: dataset.get("fields"),
      });
    }
  };

  setOriginalTags = ({
    // API format
    tags = [],
    version = null,
  } = {}) => {
    this.isTagsInEditMode = false;
    this.setState({
      tagsViewState: fromJS({
        isInProgress: false,
      }),
      tags: new OrderedMap(tags.map((tag) => [tagKeyGetter(tag), tag])),
      tagsVersion: version,
    });
  };

  setWiki = ({
    // API format
    text = "",
    version = null,
  } = {}) => {
    this.setState({
      wikiViewState: fromJS({
        isInProgress: false,
      }),
      wiki: text,
      wikiVersion: version,
    });
  };

  setFields = (response) => {
    const {
      dataset: { fields },
    } = response;
    this.setState({ fields: fromJS(fields), origFields: fromJS(fields) });
  };

  UNSAFE_componentWillMount() {
    this.handlePropsChange(undefined, this.props);
    this.props.addHasChangesHook(this.hasChanges);
    this.initFieldsOverlay(this.props.dataset);
  }

  UNSAFE_componentWillUpdate(newProps) {
    this.handlePropsChange(this.props, newProps);
    this.initFieldsOverlay(newProps.dataset);
  }

  handlePropsChange = (
    /* prevProps */ {
      entityId: prevEntityId,
      dataset: prevDataset,
      isLoadingDetails: prevIsLoadingDetails,
    } = {},
    /* newProps */ { entityId, dataset, isLoadingDetails } = {}
  ) => {
    if (prevEntityId !== entityId && entityId) {
      this.initEntity(entityId);
    }
    if (
      prevDataset?.getIn(["apiLinks", "datagraph"]) == null &&
      dataset?.getIn(["apiLinks", "datagraph"]) != null
    ) {
      this.initFields(dataset);
    }

    if (isLoadingDetails !== prevIsLoadingDetails) {
      this.setState({
        wikiViewState: fromJS({
          isInProgress: isLoadingDetails,
        }),
      });
    }
  };

  saveTags = () => {
    const { entityId, intl } = this.props;
    const { tagsVersion, tags } = this.state;
    const tagsToSave = tags.toList().toJS();

    this.isTagsInEditMode = true;
    return ApiUtils.fetch(
      `catalog/${entityId}/collaboration/tag`,
      {
        method: "POST",
        body: JSON.stringify({
          tags: tagsToSave,
          version: tagsVersion,
        }),
      },
      3
    )
      .then((response) => {
        return response.json().then(this.setOriginalTags, () => {}); //tags seem to be saved, but response json is not valid; ignore?
      })
      .catch((response) => {
        return response.json().then((e) => {
          // User-friendly error messages for CME are currently only supported on DCS
          // If on software, use the existing error message
          if (isNotSoftware() && e?.errorMessage) {
            this.props.addNotification(e?.errorMessage, "error");
          } else {
            this.setState({
              tagsViewState: this.getErrorViewState(
                intl.formatMessage({ id: "Wiki.LabelsNotSaved" })
              ),
            });
          }

          return null;
        });
      });
  };

  addTag = (tag) => {
    this.setState(({ tags }) => {
      const key = tagKeyGetter(tag);
      let newTags = tags;
      if (!tags.has(key)) {
        newTags = tags.set(key, tag.trim());
      }
      return {
        tags: newTags,
      };
    }, this.saveTags);
  };

  removeTag = (tag) => {
    const dialog = this.props.showConfirmationDialog;
    const { intl } = this.props;
    return new Promise((resolve, reject) => {
      dialog({
        title: intl.formatMessage({ id: "Wiki.RemoveLabel" }),
        cancelText: intl.formatMessage({ id: "Common.Cancel" }),
        confirmText: intl.formatMessage({ id: "Common.Remove" }),
        text: intl.formatMessage(
          { id: "Wiki.RemoveLabelConfirmation" },
          { tag: tag }
        ),
        confirm: () => {
          this.setState(
            ({ tags }) => ({
              tags: tags.delete(tagKeyGetter(tag)),
            }),
            this.saveTags
          );
          resolve();
        },
      });
    });
  };

  editWiki = (e) => {
    e.stopPropagation();
    e.preventDefault();
    this.setState({
      isWikiInEditMode: true,
      wikiSummary: false,
    });
  };

  addSummary = (e) => {
    e.stopPropagation();
    e.preventDefault();
    this.setState({
      isWikiInEditMode: true,
      wikiSummary: true,
    });
  };

  cancelWikiEdit = () => {
    this.setState({
      isWikiInEditMode: false,
      wikiViewState: fromJS({
        isInProgress: false,
      }), // reset errors if any
    });
    this.wikiChanged = false;
  };

  saveWiki = ({ text, version }) => {
    this.setState({
      isWikiInEditMode: false,
      wiki: text,
      wikiVersion: version,
    });
    this.wikiChanged = false;
  };

  isError = (response) => {
    return !response.ok && response.status !== 404; // api returns 404 for expected errors, which is weird. But for know I have to check for 404 code.
  };

  getErrorViewState = (errorMessage = "Error") =>
    fromJS({
      isFailed: true,
      error: {
        message: errorMessage,
        id: "" + Math.random(), // to show an error after re-try
      },
    });

  onChange = () => {
    this.wikiChanged = this.state.isWikiInEditMode; // this event is fired, when editMode is canceled. We should not treat this change as user change
  };

  renderWikiContent = () => {
    const { isEditAllowed, entityId, dataset } = this.props;
    const { wiki, wikiViewState } = this.state;
    const isInProgress = wikiViewState.get("isInProgress");

    if (isInProgress === undefined || isInProgress) {
      return null;
    } else if (wiki) {
      return (
        <MarkdownEditor
          entityId={entityId}
          value={wiki}
          entityType={dataset?.get("datasetType")}
          fullPath={dataset?.get("fullPath")}
          readMode
          onChange={this.onChange}
        />
      );
    } else {
      return (
        <WikiEmptyState
          onAddWiki={isEditAllowed ? this.editWiki : null}
          onAddSummary={this.addSummary}
        />
      );
    }
  };

  renderCollapseIcon = () => {
    return (
      <IconButton
        tooltip={
          !this.state.sidebarCollapsed ? "Common.Collapse" : "Common.Expand"
        }
        onClick={this.toggleRightPanel}
        className={collapseButton}
      >
        <dremio-icon
          name={
            !this.state.sidebarCollapsed
              ? "scripts/CollapseRight"
              : "scripts/CollapseLeft"
          }
        />
      </IconButton>
    );
  };

  toggleRightPanel = (e) => {
    e.stopPropagation();
    e.preventDefault();
    this.setState({
      sidebarCollapsed: !this.state.sidebarCollapsed,
    });
  };

  searchColumns = (searchVal) => {
    const { overlay, dataset, isPanel } = this.props;
    const { origFields } = this.state;
    if (searchVal === "") {
      this.setState({
        fields: origFields || new Immutable.List(),
        searchTerm: "",
      });
      return;
    }
    let columnDetails = origFields;
    if ((overlay || isPanel) && dataset?.get("fields")) {
      columnDetails = dataset?.get("fields");
    }
    const filteredColumns = columnDetails?.filter((column) =>
      column?.get("name")?.toLowerCase().includes(searchVal?.toLowerCase())
    );
    this.setState({
      fields: filteredColumns || new Immutable.List(),
      searchTerm: searchVal,
    });
  };

  render() {
    const {
      className,
      startSearch,
      entityId,
      isEditAllowed,
      dataset,
      intl,
      overlay,
      isPanel,
      hideSqlEditorIcon,
      hideGoToButton,
      handlePanelDetails,
    } = this.props;
    const {
      isWikiInEditMode,
      wiki,
      wikiVersion,
      tags,
      fields,
      sidebarCollapsed,
      searchTerm,
      tagsVersion,
    } = this.state;
    const columnDetails = fields;
    const wrapperStylesFix = {
      height: "auto", // need reset a height from 100% to auto, as we need to fit wrapper to it's content
    };
    const messageStyle = {
      top: toolbarHeight, // We should display and error below the title
    };
    const columnToolbar = [
      {
        name: "search",
        component: (
          <ShrinkableSearch
            search={this.searchColumns}
            tooltip={intl.formatMessage({ id: "Common.Search" })}
          />
        ),
        onClickHandle: () => {},
        componentClass: "collapsibleToolbarIconContainer",
      },
      // can be uncommented when edit column description is ready from backend
      // {
      //   name: "edit",
      //   tooltip: "Edit",
      //   component: (
      //     <dremio-icon
      //       key="edit"
      //       name="interface/edit"
      //       onClick={() => {
      //         this.editWiki();
      //       }}
      //       // className={collapsibleToolbarIcon}
      //     />
      //   ),
      // },
    ];
    const wikiToolbar = [];
    if (isEditAllowed && wiki)
      wikiToolbar.push({
        name: "edit",
        tooltip: intl.formatMessage({ id: "Common.Edit" }),
        component: (
          <IconButton
            data-actionid="edit-wiki"
            tooltip="Common.Edit"
            onClick={this.editWiki}
            className={collapsibleToolbarIcon}
          >
            <dremio-icon name="interface/edit" />
          </IconButton>
        ),
        componentClass: "collapsibleToolbarEditIconContainer",
      });

    // If wiki is empty we show empty content placeholder with "Add wiki" button and hide edit button in toolbar
    return (
      <WikiWrapper
        getLoadViewState={getLoadViewState}
        showLoadMask={false}
        extClassName={className}
        wikiSummary={this.state.wikiSummary}
        wikiViewState={this.state.wikiViewState}
        wrapperStylesFix={wrapperStylesFix}
        messageStyle={messageStyle}
        columnDetails={columnDetails}
        columnToolbar={columnToolbar}
        wikiToolbar={wikiToolbar}
        renderWikiContent={this.renderWikiContent}
        isWikiInEditMode={isWikiInEditMode}
        entityId={entityId}
        onChange={this.onChange}
        wiki={wiki}
        wikiVersion={wikiVersion}
        saveWiki={this.saveWiki}
        cancelWikiEdit={this.cancelWikiEdit}
        sidebarCollapsed={sidebarCollapsed}
        tagsViewState={this.state.tagsViewState}
        getTags={getTags}
        tags={tags}
        isEditAllowed={isEditAllowed}
        addTag={this.addTag}
        tagsVersion={tagsVersion}
        setOriginalTags={this.setOriginalTags}
        removeTag={this.removeTag}
        startSearch={startSearch}
        renderCollapseIcon={this.renderCollapseIcon}
        dataset={dataset}
        overlay={overlay}
        searchTerm={searchTerm}
        isPanel={isPanel}
        hideSqlEditorIcon={hideSqlEditorIcon}
        hideGoToButton={hideGoToButton}
        isPanelError={dataset?.get("error") || this.state.apiError}
        handlePanelDetails={handlePanelDetails}
      />
    );
  }
}

export const Wiki = withRouter(
  connect(mapStateToProps, (dispatch) => ({
    startSearch: startSearchAction(dispatch),
    showConfirmationDialog() {
      return dispatch(showConfirmationDialog(...arguments));
    },
    addNotification() {
      return dispatch(addNotification(...arguments));
    },
  }))(withRouteLeaveSubscription(injectIntl(WikiView)))
);
