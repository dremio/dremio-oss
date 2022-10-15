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
import { connect } from "react-redux";
import classNames from "classnames";
import PropTypes from "prop-types";

import { stopPropagation } from "@app/utils/reactEventUtils";
import { setEntityActiveState } from "@app/reducers/home/pinnedEntities";
import { isEntityPinned } from "@app/selectors/home";
import FilledSmallPin from "@app/art/FilledSmallPin.svg";
import HollowSmallPin from "@app/art/HollowSmallPin.svg";
import "./ResourcePin.less";

const mapStateToProps = (state, { entityId }) => ({
  isPinned: isEntityPinned(state, entityId),
});

const mapDispatchToProps = {
  toggleActivePin: setEntityActiveState,
};

//export for tests
export class ResourcePin extends PureComponent {
  static propTypes = {
    //public api
    entityId: PropTypes.string.isRequired,

    // connected
    isPinned: PropTypes.bool.isRequired,

    toggleActivePin: PropTypes.func, // (entityId: string, isPinned: bool): void
  };

  onPinClick = (e) => {
    stopPropagation(e);
    const { entityId, isPinned, toggleActivePin } = this.props;
    toggleActivePin(entityId, !isPinned);
  };

  render() {
    const { isPinned } = this.props;
    const pinClass = classNames("pin", { active: isPinned });
    return (
      <span className={pinClass} onClick={this.onPinClick} aria-label="Pin">
        {isPinned ? <FilledSmallPin /> : <HollowSmallPin />}
      </span>
    );
  }
}

export default connect(mapStateToProps, mapDispatchToProps)(ResourcePin);
