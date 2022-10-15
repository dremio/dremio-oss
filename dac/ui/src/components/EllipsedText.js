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
import { Component, forwardRef } from "react";
import PropTypes from "prop-types";
import "./EllipsedText.css";

class EllipsedTextComponent extends Component {
  static propTypes = {
    text: PropTypes.string,
    className: PropTypes.string,
    children: PropTypes.node,
    title: PropTypes.any,
    innerRef: PropTypes.any,
  };

  render() {
    const { text, children, className, title, innerRef, ...props } = this.props;
    return (
      <div
        ref={innerRef}
        className={"EllipsedText " + (className || "")}
        title={title}
        {...props}
      >
        {children || text}
      </div>
    );
  }
}

const EllipsedText = forwardRef((props, ref) => (
  <EllipsedTextComponent {...props} innerRef={ref} />
));
EllipsedText.displayName = "EllipsedText"; //For enzyme tests

export default EllipsedText;
