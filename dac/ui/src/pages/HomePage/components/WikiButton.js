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
import { IconButton } from "dremio-ui-lib";
import { wikiButton } from "./WikiButton.less";

export class WikiButtonView extends PureComponent {
  static propTypes = {
    onClick: PropTypes.func,
    className: PropTypes.string,
  };

  render() {
    const { className, onClick } = this.props;

    return (
      <IconButton
        tooltip={la("Wiki")}
        onClick={onClick}
        className={`${wikiButton} ${className}`}
      >
        <dremio-icon name="interface/sidebar" />
      </IconButton>
    );
  }
}

export class WikiButton extends Component {
  static propTypes = {
    onClick: PropTypes.func,
    className: PropTypes.string,
  };
  render() {
    const { onClick, className } = this.props;

    const props = {
      onClick,
      className,
    };

    return <WikiButtonView {...props} />;
  }
}
