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
import { connect } from "react-redux";
import { FormattedMessage, injectIntl } from "react-intl";
import { getExploreState } from "@app/selectors/explore";

import { showConfirmationDialog } from "actions/confirmation";
import { resetNewQuery } from "actions/explore/view";

import { getLocation } from "selectors/routing";

import { EXPLORE_VIEW_ID } from "reducers/explore/view";

import FontIcon from "components/Icon/FontIcon";

import { parseResourceId } from "utils/pathUtils";

import * as classes from "./NewQueryButton.module.less";

export class NewQueryButton extends Component {
  static propTypes = {
    location: PropTypes.object.isRequired,
    currentSql: PropTypes.string,

    showConfirmationDialog: PropTypes.func,
    resetNewQuery: PropTypes.func,
    intl: PropTypes.object.isRequired,
    fromSideBar: PropTypes.bool,
  };

  static contextTypes = {
    username: PropTypes.string,
    router: PropTypes.object.isRequired,
  };

  constructor(props) {
    super(props);
    this.state = {};
  }

  getNewQueryHref() {
    const { location } = this.props;
    const { username } = this.context;
    const resourceId = parseResourceId(location.pathname, username);
    return "/new_query?context=" + encodeURIComponent(resourceId);
  }

  handleClick = (e) => {
    const { location, currentSql, intl } = this.props;
    if (e.metaKey || e.ctrlKey) {
      // DX-10607, DX-11299 pass to default link behaviour, when cmd/ctrl is pressed on click
      return;
    }
    if (location.pathname === "/new_query") {
      if (currentSql && currentSql.trim()) {
        this.props.showConfirmationDialog({
          title: intl.formatMessage({ id: "Common.UnsavedWarning" }),
          text: [
            intl.formatMessage({ id: "NewQuery.UnsavedChangesWarning" }),
            intl.formatMessage({ id: "NewQuery.UnsavedChangesWarningPrompt" }),
          ],
          confirmText: intl.formatMessage({ id: "Common.Continue" }),
          cancelText: intl.formatMessage({ id: "Common.Cancel" }),
          confirm: () => {
            this.props.resetNewQuery(EXPLORE_VIEW_ID);
          },
        });
      } else {
        this.props.resetNewQuery(EXPLORE_VIEW_ID); // even if there's no SQL, clear any errors
      }
    } else {
      this.context.router.push(this.getNewQueryHref());
    }
    e.preventDefault();
  };

  render() {
    const { fromSideBar, intl } = this.props;
    if (fromSideBar) {
      const { mouseOver } = this.state;
      let iconType = "PlusSignGray";
      if (mouseOver) {
        iconType = "PlusSign";
      }

      return (
        <div
          className="new-query-button"
          style={styles.baseSideBar}
          onMouseEnter={this.mouseEnter}
          onMouseLeave={this.mouseLeave}
        >
          <a
            href={this.getNewQueryHref()}
            data-qa="new-query-button"
            onClick={this.handleClick}
            className={classes["new-query-button__linkSideBar"]}
          >
            <FontIcon
              theme={styles.iconSideBar}
              type={iconType}
              tooltip={intl.formatMessage({ id: "NewQuery.NewQuery" })}
            />
          </a>
        </div>
      );
    }

    return (
      <div className="new-query-button" style={styles.base}>
        <a
          href={this.getNewQueryHref()}
          data-qa="new-query-button"
          onClick={this.handleClick}
          className={classes["new-query-button__link"]}
        >
          <FontIcon theme={styles.icon} type="PlusSign" />
          <FormattedMessage id="NewQuery.NewQuery" />
        </a>
      </div>
    );
  }
}
NewQueryButton = injectIntl(NewQueryButton);

function mapStateToProps(state) {
  const explorePage = getExploreState(state); //todo explore page state should not be here
  return {
    location: getLocation(state),
    currentSql: explorePage ? explorePage.view.currentSql : null,
  };
}

export default connect(mapStateToProps, {
  showConfirmationDialog,
  resetNewQuery,
})(NewQueryButton);

const styles = {
  base: {
    display: "flex",
    alignItems: "center",
  },
  baseSideBar: {
    width: 45,
    height: 45,
  },
  icon: {
    Icon: {
      width: "1.05em",
      height: "1.05em",
    },
    Container: {
      height: "1.05em",
      display: "inline-block",
      verticalAlign: "-0.4em",
      marginRight: 9,
      fontSize: "inherit",
    },
  },
  iconSideBar: {
    Icon: {
      width: "1.05em",
      height: "1.05em",
    },
    Container: {
      display: "inline-block",
      fontSize: "inherit",
    },
  },
};
