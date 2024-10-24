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
import { PureComponent, Component } from "react";
import PropTypes from "prop-types";
import { IconButton } from "dremio-ui-lib/components";
import { wikiButton } from "./WikiButton.less";
import { intl } from "#oss/utils/intl";

export class WikiButtonView extends PureComponent {
  static propTypes = {
    onClick: PropTypes.func,
    className: PropTypes.string,
    expanded: PropTypes.bool,
    wikiViewRef: PropTypes.any,
  };

  render() {
    const { className, onClick, expanded, wikiViewRef } = this.props;

    if (expanded) {
      return (
        <>
          <IconButton
            tooltip={intl.formatMessage({ id: "Wiki.Expand" })}
            onClick={wikiViewRef?.expandWiki}
            tooltipPortal
            tooltipPlacement="top"
            className={`${wikiButton} ${className}`}
          >
            <dremio-icon name="interface/expand" />
          </IconButton>
          <IconButton
            tooltip={intl.formatMessage({ id: "Wiki.Edit" })}
            onClick={wikiViewRef?.editWiki}
            tooltipPortal
            tooltipPlacement="top"
            className={`${wikiButton} ${className}`}
          >
            <dremio-icon name="interface/edit" />
          </IconButton>
          <IconButton
            tooltip={intl.formatMessage({ id: "Wiki.Close" })}
            onClick={onClick}
            tooltipPortal
            tooltipPlacement="top"
            className={`${wikiButton} ${className}`}
          >
            <dremio-icon name="interface/close-big" />
          </IconButton>
        </>
      );
    } else {
      return (
        <IconButton
          tooltip={laDeprecated("Wiki")}
          onClick={onClick}
          tooltipPortal
          tooltipPlacement="top"
          className={`${wikiButton} ${className}`}
        >
          <dremio-icon name="interface/sidebar" data-qa="edit-wiki-content" />
        </IconButton>
      );
    }
  }
}

export class WikiButton extends Component {
  static propTypes = {
    onClick: PropTypes.func,
    className: PropTypes.string,
    expanded: PropTypes.bool,
    wikiViewRef: PropTypes.any,
  };
  render() {
    const { onClick, className, expanded, wikiViewRef } = this.props;

    const props = {
      onClick,
      className,
      expanded,
      wikiViewRef,
    };

    return <WikiButtonView {...props} />;
  }
}
