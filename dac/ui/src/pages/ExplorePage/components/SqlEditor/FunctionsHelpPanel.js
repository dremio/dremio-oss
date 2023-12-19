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
import { createRef, PureComponent } from "react";
import PropTypes from "prop-types";
import HelpFunctions from "./HelpFunctions";

import "./FunctionsHelpPanel.less";

const HEADER_LIST_OF_FUNCS = 36;

class FunctionsHelpPanel extends PureComponent {
  static propTypes = {
    height: PropTypes.number,
    dragType: PropTypes.string.isRequired,
    isVisible: PropTypes.bool.isRequired,
    addFuncToSqlEditor: PropTypes.func,
    handleSidebarCollapse: PropTypes.func,
  };

  constructor(props) {
    super(props);
    this.sqlHelpPanelRef = createRef();
    this.state = {
      heightPanel: 110,
    };
  }

  componentDidMount() {
    this.setPanelHeight();
  }

  UNSAFE_componentWillReceiveProps() {
    const { current: { offsetHeight } = {} } = this.sqlHelpPanelRef;
    if (offsetHeight && this.state.heightPanel !== offsetHeight) {
      this.setPanelHeight();
    }
  }

  setPanelHeight() {
    const { current: { offsetHeight } = {} } = this.sqlHelpPanelRef;
    this.setState({
      heightPanel: offsetHeight - HEADER_LIST_OF_FUNCS,
    });
  }

  render() {
    const { isVisible, dragType, height } = this.props;
    return (
      <div
        className="sql-help-panel"
        onClick={(e) => e.preventDefault()}
        style={{ height: isVisible ? height : 0 }}
        ref={this.sqlHelpPanelRef}
      >
        {isVisible && (
          <HelpFunctions
            dragType={dragType}
            heightPanel={this.state.heightPanel}
            addFuncToSqlEditor={this.props.addFuncToSqlEditor}
          />
        )}
      </div>
    );
  }
}
export default FunctionsHelpPanel;
