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
import { useEffect, useImperativeHandle, useRef, useState } from "react";
import { connect } from "react-redux";
import { withRouter } from "react-router";
import {
  usePopupState,
  bindToggle,
  bindPopper,
} from "material-ui-popup-state/hooks";
import { Popover } from "@mui/material";
import { FormattedMessage } from "react-intl";
import DatePicker, { ReactDatePicker } from "react-datepicker";

import { Button, DialogContent } from "dremio-ui-lib/dist-esm";

import {
  fetchCommitBeforeTime as fetchCommitBeforeTimeAction,
  setReference as setReferenceAction,
  type SetReferenceAction,
} from "@app/actions/nessie/nessie";
import { LogEntry } from "@app/services/nessie/client";
import { Reference } from "@app/types/nessie";
import Radio from "@mui/material/Radio";
import RadioGroup from "@mui/material/RadioGroup";
import FormControlLabel from "@mui/material/FormControlLabel";
import FormControl from "@mui/material/FormControl";
import { SearchField } from "@app/components/Fields";
import { useNessieContext } from "@app/pages/NessieHomePage/utils/context";
import BranchList from "./components/BranchList/BranchList";
import CommitBrowser from "./components/CommitBrowser/CommitBrowser";
import BranchPickerTag from "./components/BranchPickerTag/BranchPickerTag";
import RefIcon from "./components/RefIcon/RefIcon";
import { useBranchPickerContext } from "./utils";

import "./BranchPicker.less";

const RADIO_HEAD = "head";
const RADIO_DATE = "date";
const RADIO_COMMIT = "commit";

const defaultPosition: any = {
  anchorOrigin: { horizontal: "right", vertical: "bottom" },
  transformOrigin: { horizontal: "left", vertical: "center" },
};

type ConnectedProps = {
  setReference: typeof setReferenceAction;
  fetchCommitBeforeTime: typeof fetchCommitBeforeTimeAction;
};

type BranchPickerProps = {
  redirectUrl?: string;
  getAnchorEl?: () => HTMLElement | undefined;
  position?: any;
  onApply?: () => void;
};

function BranchPicker({
  router,
  redirectUrl = "",
  setReference,
  fetchCommitBeforeTime,
  getAnchorEl,
  position = defaultPosition,
  onApply = () => {},
}: BranchPickerProps & ConnectedProps & { router?: any }) {
  const { ref } = useBranchPickerContext();
  const { state, api, stateKey } = useNessieContext();
  const [refState, setRefState] = useState<SetReferenceAction["payload"]>({
    reference: state.reference,
    hash: state.hash,
    date: state.date,
  });
  const popupState = usePopupState({
    variant: "popover",
    popupId: "branchPickerPopover",
  });
  const toggleProps = bindToggle(popupState);

  useImperativeHandle(ref, () => popupState);

  function submitForm() {
    setReference(refState, stateKey);
    if (redirectUrl) router.push(redirectUrl);
    if (onApply) onApply();
    closeDialog();
  }

  function closeDialog() {
    popupState.close();
  }

  function changeReference(newReference?: Reference | null) {
    if (!newReference) return;
    setRefState({ reference: newReference });
    setFormVal(RADIO_HEAD);
  }

  function setHash(logEntry: LogEntry) {
    const newHash = logEntry.commitMeta?.hash;
    if (newHash) {
      setRefState({ reference, hash: newHash });
    }
  }

  const [formVal, setFormVal] = useState(RADIO_HEAD);
  const pickerRef = useRef<ReactDatePicker>(null);

  function showPicker() {
    const picker = pickerRef.current;
    if (state.date || !picker) return;
    if (!picker.isCalendarOpen()) picker.setOpen(true);
  }

  function hidePicker() {
    const picker = pickerRef.current;
    if (!picker) return;
    if (picker.isCalendarOpen()) picker.setOpen(false);
  }

  function onFormChange(value: string) {
    setFormVal(value);
    if (value === RADIO_HEAD) {
      changeReference(state.reference); //Not sure what this does
    }
  }

  async function onChooseDate(newDate: any, e: any) {
    if (!newDate) return;
    const resultState = await fetchCommitBeforeTime(
      state.reference,
      newDate,
      stateKey,
      api
    );
    if (resultState) {
      setRefState(resultState as any);
      // Picker will be replaced in https://dremio.atlassian.net/browse/DX-53586
      if (!e) hidePicker(); // Workaround to close picker after time select: event is undefined when clicking time
    }
  }

  useEffect(() => {
    if (!popupState.isOpen) return;
    setFormVal(state.hash ? RADIO_COMMIT : RADIO_HEAD);
  }, [popupState.isOpen, state.hash]);

  const { reference, hash, date } = refState;
  if (!state.reference) return null; //Loading

  return (
    <div
      onClick={(e: any) => {
        e.stopPropagation();
        e.preventDefault();
      }}
      onContextMenu={(e: any) => {
        e.stopPropagation();
      }}
    >
      <div
        className="branchPicker"
        {...toggleProps}
        onClick={(e: any) => {
          toggleProps.onClick(e);
        }}
      >
        <BranchPickerTag
          reference={state.reference}
          hash={state.hash}
          isOpen={popupState.isOpen}
          rightContent={<dremio-icon name="interface/right-chevron" />}
        />
      </div>
      {reference && state.defaultReference && (
        <Popover
          {...bindPopper(popupState)}
          transitionDuration={200}
          {...position}
          {...(getAnchorEl && { anchorEl: getAnchorEl })}
        >
          <div className="branchPicker-popup">
            <DialogContent
              actions={
                <>
                  <Button variant="secondary" onClick={closeDialog}>
                    <FormattedMessage id="Common.Cancel" />
                  </Button>
                  <Button variant="primary" onClick={submitForm}>
                    <FormattedMessage id="Common.Apply" />
                  </Button>
                </>
              }
              title={state.reference.name}
              icon={
                <RefIcon
                  reference={state.reference}
                  hash={state.hash}
                  style={{}}
                />
              }
            >
              <div className="branchesView">
                <div className="branchesView-left">
                  <BranchList
                    defaultReference={state.defaultReference}
                    currentReference={reference}
                    onClick={changeReference}
                  />
                </div>
                <div className="branchesView-right">
                  {!!reference && (
                    <FormControl
                      component="fieldset"
                      className="branchesView-radios"
                    >
                      <RadioGroup
                        value={formVal}
                        onClick={(e) => {
                          e.stopPropagation();
                        }}
                        onChange={({ target: { value } }: any) => {
                          onFormChange(value);
                        }}
                      >
                        <FormControlLabel
                          className="branchesView-radioLabel"
                          value={RADIO_HEAD}
                          control={<Radio />}
                          label="Head"
                        />
                        <FormControlLabel
                          onClick={showPicker}
                          className="branchesView-radioLabel"
                          value={RADIO_DATE}
                          control={<Radio />}
                          label={
                            <div className="branchesView-datePicker">
                              <FormattedMessage id="BranchPicker.ChooseTime" />
                              <DatePicker
                                ref={pickerRef}
                                disabled={formVal !== "date"}
                                showTimeSelect
                                selected={date}
                                onChange={onChooseDate}
                                popperPlacement="bottom-start"
                                customInput={<SearchField showIcon={false} />}
                                dateFormat="M/d/yyyy hh:mm aa"
                              />
                            </div>
                          }
                        />
                        <FormControlLabel
                          className="branchesView-radioLabel branchesView-commitBrowserContainer"
                          value={RADIO_COMMIT}
                          control={<Radio />}
                          label={
                            <>
                              <span className="branchesView-commitLabel">
                                <FormattedMessage id="Common.Commit" />
                              </span>
                              <CommitBrowser
                                disabled={formVal !== "commit"}
                                key={reference.name}
                                branch={reference}
                                onClick={setHash}
                                selectedHash={hash}
                                api={api}
                              />
                            </>
                          }
                        />
                      </RadioGroup>
                    </FormControl>
                  )}
                </div>
              </div>
            </DialogContent>
          </div>
        </Popover>
      )}
    </div>
  );
}

const mapDispatchToProps = {
  setReference: setReferenceAction,
  fetchCommitBeforeTime: fetchCommitBeforeTimeAction,
};

export default withRouter(
  //@ts-ignore
  connect(null, mapDispatchToProps)(BranchPicker)
);
