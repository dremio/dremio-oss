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
import classNames from "classnames";
import { createSelector } from "reselect";
import { OrderedMap, fromJS } from "immutable";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import { isDcsEdition } from "dyn-load/utils/versionUtils";
import { withRouteLeaveSubscription } from "@app/containers/RouteLeave.js";
import { TagsView } from "@app/pages/ExplorePage/components/TagsEditor/Tags.js";
import ViewStateWrapper from "@app/components/ViewStateWrapper";
import MarkdownEditor from "@app/components/MarkdownEditor";
import { toolbarHeight as toolbarHeightCssValue } from "@app/components/MarkdownEditor.less";
import { startSearch as startSearchAction } from "actions/search";
import ApiUtils from "@app/utils/apiUtils/apiUtils";
import { showConfirmationDialog } from "@app/actions/confirmation";
import { addNotification } from "@app/actions/notification";
import { DataColumnList } from "@app/pages/ExplorePage/components/DataColumns/DataColumnList";
import { WikiModal } from "@app/pages/ExplorePage/components/Wiki/WikiModal";
import WikiEmptyState from "@app/components/WikiEmptyState";
import { getTableColumns, isWikAvailable } from "@app/selectors/explore";
import { SectionTitle, getIconButtonConfig } from "./SectionTitle";
import ImmutablePropTypes from "react-immutable-proptypes";
import { columnPropTypes } from "@app/pages/ExplorePage/components/DataColumns/DataColumn";

import {
  leftColumn,
  sectionsContainer,
  sectionItem,
  tags as tagsCls,
  rightColumn,
  layout,
  sectionTitle as sectionTitleCls,
} from "./Wiki.less";

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

const mapStateToProps = (state, { location }) => {
  const loc = state.routing.locationBeforeTransitions || {};
  return {
    showLoadMask: !isWikAvailable(state, location), // means either dataset data is not loaded yet or we should not get to the wiki page for current data set
    columns: getTableColumns(state, loc.query && loc.query.version, loc),
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
    columns: ImmutablePropTypes.listOf(
      ImmutablePropTypes.contains(columnPropTypes)
    ),
  };

  static defaultProps = {
    showWikiContent: true,
    showTags: true,
    columns: fromJS([]),
  };

  // state and related properties ----------------------------
  state = {
    isWikiInEditMode: false,
    wikiViewState: fromJS({}),
    wiki: "",
    wikiVersion: null,

    // Tags
    ...defaultTagsState,
    tagsViewState: fromJS({}),

    // Fields
    fields: fromJS([]),
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

    //load tags and wiki
    ApiUtils.fetchJson(
      `catalog/${entityId}/collaboration/tag`,
      this.setOriginalTags,
      (error) => {
        if (this.isError(error)) {
          this.setState({
            tagsViewState: this.getErrorViewState(la("Failed to load Tags.")),
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
            wikiViewState: this.getErrorViewState(la("Failed to load Wiki.")),
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
    this.setState({ fields: fromJS(fields) });
  };

  componentWillMount() {
    this.handlePropsChange(undefined, this.props);
    this.props.addHasChangesHook(this.hasChanges);
  }

  componentWillUpdate(newProps) {
    this.handlePropsChange(this.props, newProps);
  }

  handlePropsChange = (
    /* prevProps */ { entityId: prevEntityId, dataset: prevDataset } = {},
    /* newProps */ { entityId, dataset, columns } = {}
  ) => {
    if (prevEntityId !== entityId && entityId) {
      this.initEntity(entityId);
    }

    if (
      columns?.size === 0 &&
      prevDataset?.getIn(["apiLinks", "datagraph"]) == null &&
      dataset?.getIn(["apiLinks", "datagraph"]) != null
    ) {
      this.initFields(dataset);
    }
  };

  saveTags = () => {
    const { entityId } = this.props;
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
          if (isDcsEdition() && e?.errorMessage) {
            this.props.addNotification(e?.errorMessage, "error");
          } else {
            this.setState({
              tagsViewState: this.getErrorViewState(
                la("Error: Tags are not saved")
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
    return new Promise((resolve, reject) => {
      dialog({
        title: la("Remove tag"),
        cancelText: la("Cancel"),
        confirmText: la("Remove"),
        text: la(`Are you sure that you want to remove '${tag}' tag?`),
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

  editWiki = () => {
    this.setState({
      isWikiInEditMode: true,
    });
  };

  cancelWikiEdit = () => {
    this.setState({
      isWikiInEditMode: false,
      wikiViewState: fromJS({}), // reset errors if any
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
      return <MarkdownEditor value={wiki} readMode onChange={this.onChange} />;
    } else {
      return (
        <WikiEmptyState onAddWiki={isEditAllowed ? this.editWiki : null} />
      );
    }
  };

  render() {
    const {
      className,
      startSearch,
      entityId,
      isEditAllowed,
      showLoadMask,
      showTags = true,
      showWikiContent = true,
      columns,
    } = this.props;
    const { isWikiInEditMode, wiki, wikiVersion, tags, fields } = this.state;
    const wrapperStylesFix = {
      height: "auto", // need reset a height from 100% to auto, as we need to fit wrapper to it's content
    };
    const messageStyle = {
      top: toolbarHeight, // We should display and error below the title
    };

    // If wiki is empty we show empty content placeholder with "Add wiki" button and hide edit button in toolbar
    return (
      <ViewStateWrapper
        viewState={getLoadViewState(showLoadMask)}
        style={{ height: "auto", display: "flex", flex: 1, minHeight: 0 }}
      >
        <div className={layout} data-qa="wikiSection">
          {showWikiContent && (
            <div
              className={classNames(leftColumn, className)}
              data-qa="wikiWrapper"
            >
              <ViewStateWrapper
                viewState={this.state.wikiViewState}
                style={wrapperStylesFix}
                hideChildrenWhenFailed={false}
                messageStyle={messageStyle}
              >
                <SectionTitle
                  className={sectionTitleCls}
                  title={la("Wiki")}
                  buttons={
                    isEditAllowed && wiki
                      ? [
                          getIconButtonConfig({
                            key: "edit",
                            icon: "interface/edit",
                            altText: "Edit",
                            onClick: this.editWiki,
                            styles: {
                              width: 17,
                              height: 18,
                              color: "var(--dremio--color--neutral--600)",
                            },
                          }),
                        ]
                      : null
                  }
                />
                {this.renderWikiContent()}
                <WikiModal
                  isOpen={isWikiInEditMode}
                  entityId={entityId}
                  onChange={this.onChange}
                  wikiValue={wiki}
                  wikiVersion={wikiVersion}
                  save={this.saveWiki}
                  cancel={this.cancelWikiEdit}
                />
              </ViewStateWrapper>
            </div>
          )}
          <div
            className={classNames(rightColumn, sectionsContainer)}
            data-qa="tagsSection"
          >
            {showTags && (
              <ViewStateWrapper
                viewState={this.state.tagsViewState}
                className={sectionItem}
                style={wrapperStylesFix}
                hideChildrenWhenFailed={false}
                messageStyle={messageStyle}
              >
                <SectionTitle title={la("Tags")} className={sectionTitleCls} />
                <TagsView
                  className={tagsCls}
                  tags={getTags(tags)}
                  onAddTag={isEditAllowed ? this.addTag : null}
                  onRemoveTag={isEditAllowed ? this.removeTag : null}
                  onTagClick={startSearch}
                />
              </ViewStateWrapper>
            )}
            <DataColumnList
              className={sectionItem}
              columns={columns?.size ? columns : fields}
            />
          </div>
        </div>
      </ViewStateWrapper>
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
  }))(withRouteLeaveSubscription(WikiView))
);
