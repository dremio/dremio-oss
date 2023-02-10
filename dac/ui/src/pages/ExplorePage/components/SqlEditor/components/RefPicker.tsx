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
import { useMemo, useRef } from "react";
import { FormattedMessage } from "react-intl";
import { connect } from "react-redux";

import { Menu, MenuItem } from "@mui/material";
import {
  usePopupState,
  bindToggle,
  bindPopper,
} from "material-ui-popup-state/hooks";

import { isDataPlaneEnabled } from "@inject/utils/dataPlaneUtils";
import { getSortedSources } from "@app/selectors/home";
import FontIcon from "@app/components/Icon/FontIcon";
import { NESSIE_REF_PREFIX } from "@app/constants/nessie";
import SourceBranchPicker from "@app/pages/HomePage/components/SourceBranchPicker/SourceBranchPicker";
import {
  BranchPickerContext,
  useContextValue,
} from "@app/pages/HomePage/components/BranchPicker/utils";
import CurrentRefInfo from "./CurrentRefInfo/CurrentRefInfo";
import { getIconStatusDatabase } from "@app/utils/iconUtils";
import { isVersionedSource } from "@app/utils/sourceUtils";

import "./RefPicker.less";

type ConnectedProps = {
  sources: any[];
};
type RefPickerProps = {
  hide?: boolean;
  contextClassName?: string;
};

const iconStyle = { width: "20px", height: "20px" };
const position = {
  anchorOrigin: { horizontal: "right", vertical: "bottom" },
  transformOrigin: { horizontal: "right", vertical: "top" },
};
const branchPickerPosition = {
  anchorOrigin: { horizontal: "left", vertical: "top" },
  transformOrigin: { horizontal: "right", vertical: "top" },
};

function RefPickerItem({
  source,
  getAnchorEl,
}: {
  getAnchorEl: () => HTMLElement;
  source: any;
}) {
  const context = useContextValue();
  const ref = context.ref;
  return (
    <BranchPickerContext.Provider value={context}>
      <div
        className="refPicker-root"
        onClick={(e) => {
          e.stopPropagation();
          if (!ref.current) return;
          ref.current.open();
        }}
      >
        <span className="refPicker">
          <span className="refPicker-namewrap">
            <FontIcon
              type={getIconStatusDatabase(source.state.status, source.type)}
              theme={{ Icon: iconStyle }}
            />
            <span className="refPicker-name text-ellipsis" title={source.name}>
              {source.name}
            </span>
          </span>
          <span
            onClick={(e: any) => {
              e.stopPropagation();
            }}
            className="refPicker-branchPicker"
          >
            <SourceBranchPicker
              source={source}
              getAnchorEl={getAnchorEl}
              position={branchPickerPosition}
              prefix={NESSIE_REF_PREFIX}
              redirect={false}
            />
          </span>
        </span>
        <span className="refPicker-arrow">
          <dremio-icon
            name="interface/right-chevron"
            class="refPicker-arrow--icon"
          />
        </span>
      </div>
    </BranchPickerContext.Provider>
  );
}

function RefPicker({
  sources,
  hide,
  contextClassName,
}: RefPickerProps & ConnectedProps) {
  const popupState = usePopupState({
    variant: "popper",
    popupId: "refPickerMenu",
  });
  const toggleProps = bindToggle(popupState);
  const handleClose = toggleProps.onClick;

  const dataPlaneSources = useMemo(
    () => sources.filter((cur) => isVersionedSource(cur.type)),
    [sources]
  );
  const ref = useRef(null);
  const menuRef = useRef<any>(null);
  const anchorEl = popupState.isOpen ? ref.current : null;

  if (!isDataPlaneEnabled || dataPlaneSources.length === 0 || hide) return null;

  return (
    <div className="sqlAutocomplete__context">
      <span className="refInfo-wrapper" ref={ref}>
        <span className="refInfo-prefix">Ref:</span>
        <span
          {...toggleProps}
          className={`${contextClassName} refInfo-content`}
        >
          <CurrentRefInfo sources={dataPlaneSources} />
        </span>
      </span>
      <Menu
        disableAutoFocusItem
        keepMounted
        {...bindPopper(popupState)}
        anchorEl={anchorEl}
        {...(position as any)}
        onClose={handleClose}
      >
        <div className="refPicker-popup" ref={menuRef}>
          <span className="refPicker-popup-title">
            <FormattedMessage id="Nessie.SetReferences" />
          </span>
          {dataPlaneSources.map((source, i) => (
            <MenuItem key={i}>
              <RefPickerItem
                source={source}
                getAnchorEl={() =>
                  menuRef.current
                    ? menuRef.current?.parentElement?.parentElement ||
                      menuRef.current
                    : undefined
                }
              />
            </MenuItem>
          ))}
        </div>
      </Menu>
    </div>
  );
}
const mapStateToProps = (state: any) => ({
  sources: getSortedSources(state).toJS(), //Immutable.List -> Array
});

export default connect(mapStateToProps)(RefPicker);
