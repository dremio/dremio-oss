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
import PropTypes from "prop-types";
import { createSelector } from "reselect";
import { OrderedMap, fromJS } from "immutable";
import Immutable from "immutable";
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
import { injectIntl } from "react-intl";

import { editor, collapsibleToolbarIcon, collapseButton } from "./Wiki.less";
import WikiWrapper from "./WikiModal/WikiWrapper";
import ShrinkableSearch from "./ShrinkableSearch/ShrinkableSearch";

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
    showWikiContent: PropTypes.bool,
    showTags: PropTypes.bool,
    addNotification: PropTypes.func,
    dataset: PropTypes.instanceOf(Immutable.Map),
    intl: PropTypes.object.isRequired,
    overlay: PropTypes.bool,
  };

  static defaultProps = {
    showWikiContent: true,
    showTags: true,
  };

  // state and related properties ----------------------------
  state = {
    isWikiInEditMode: false,
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
    });
    const { intl } = this.props;
    //load tags and wiki
    ApiUtils.fetchJson(
      `catalog/${entityId}/collaboration/tag`,
      this.setOriginalTags,
      (error) => {
        if (this.isError(error)) {
          this.setState({
            tagsViewState: this.getErrorViewState(
              intl.formatMessage({ id: "Wiki.FailedTags" })
            ),
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
        if (this.isError(error)) {
          this.setState({
            wikiViewState: this.getErrorViewState(
              intl.formatMessage({ id: "Wiki.FailedWiki" })
            ),
          });
        } else {
          // init state with default value
          this.setWiki();
        }
      }
    );
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
    if (this.props.overlay) {
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

  componentWillMount() {
    this.handlePropsChange(undefined, this.props);
    this.props.addHasChangesHook(this.hasChanges);
    this.initFieldsOverlay(this.props.dataset);
  }

  componentWillUpdate(newProps) {
    this.handlePropsChange(this.props, newProps);
  }

  handlePropsChange = (
    /* prevProps */ { entityId: prevEntityId, dataset: prevDataset } = {},
    /* newProps */ { entityId, dataset } = {}
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
                intl.formatMessage({ id: "Wiki.TagsNotSaved" })
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
        title: intl.formatMessage({ id: "Wiki.RemoveTag" }),
        cancelText: intl.formatMessage({ id: "Common.Cancel" }),
        confirmText: intl.formatMessage({ id: "Common.Remove" }),
        text: intl.formatMessage(
          { id: "Wiki.RemoveTagConfirmation" },
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
        cancel: reject,
      });
    });
  };

  editWiki = (e) => {
    e.stopPropagation();
    e.preventDefault();
    this.setState({
      isWikiInEditMode: true,
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
    const { isEditAllowed } = this.props;
    const { wiki, wikiViewState } = this.state;
    const isInProgress = wikiViewState.get("isInProgress");

    if (isInProgress === undefined || isInProgress) {
      return null;
    } else if (wiki) {
      return (
        <MarkdownEditor
          value={wiki}
          readMode
          onChange={this.onChange}
          className={editor} // todo
        />
      );
    } else {
      return (
        <WikiEmptyState onAddWiki={isEditAllowed ? this.editWiki : null} />
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
    const { overlay, dataset } = this.props;
    const { origFields } = this.state;
    if (searchVal === "") {
      this.setState({
        fields: origFields || new Immutable.List(),
        searchTerm: "",
      });
      return;
    }
    let columnDetails = origFields;
    if (overlay) {
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
      showTags = true,
      showWikiContent = true,
      dataset,
      intl,
      overlay,
    } = this.props;
    const {
      isWikiInEditMode,
      wiki,
      wikiVersion,
      tags,
      fields,
      sidebarCollapsed,
      searchTerm,
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
        showWikiContent={showWikiContent}
        extClassName={className}
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
        removeTag={this.removeTag}
        startSearch={startSearch}
        showTags={showTags}
        renderCollapseIcon={this.renderCollapseIcon}
        dataset={dataset}
        overlay={overlay}
        searchTerm={searchTerm}
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
