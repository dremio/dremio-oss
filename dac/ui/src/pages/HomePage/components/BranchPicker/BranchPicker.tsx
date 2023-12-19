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
import ClickAwayListener from "react-click-away-listener";
import { FormattedMessage } from "react-intl";
import DatePicker, { ReactDatePicker } from "react-datepicker";

import { Button, DialogContent } from "dremio-ui-lib/components";

import {
  fetchCommitBeforeTime as fetchCommitBeforeTimeAction,
  setReference as setReferenceAction,
  type SetReferenceAction,
} from "@app/actions/nessie/nessie";
import {
  LogEntryV2 as LogEntry,
  LogResponseV2 as LogResponse,
} from "@app/services/nessie/client";
import { type Reference } from "@app/types/nessie";
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
import { getShortHash } from "@app/utils/nessieUtils";

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
  onApply?: (stateKey: string, state: SetReferenceAction["payload"]) => void;
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
  const { state, apiV2, stateKey } = useNessieContext();
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

  useEffect(() => {
    setRefState({
      reference: state.reference,
      hash: state.hash,
      date: state.date,
    });
  }, [state]);

  useImperativeHandle(ref, () => popupState);

  function submitForm() {
    setReference(refState, stateKey);
    if (redirectUrl) router.push(redirectUrl);
    if (onApply) onApply(stateKey, refState);
    popupState.close();
  }

  function cancel() {
    popupState.close();
    setRefState({
      reference: state.reference,
      hash: state.hash,
      date: state.date,
    });
  }

  function changeReference(newReference?: Reference | null) {
    if (!newReference) return;
    setRefState({ reference: newReference });
    setFormVal(RADIO_HEAD);
  }

  function setHash(logEntry: LogEntry | undefined) {
    const newHash = logEntry?.commitMeta?.hash;
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
      changeReference(reference);
    } else if (value === RADIO_COMMIT && !refState.hash) {
      setHash(firstCommit.current);
    }
  }

  async function onChooseDate(newDate: any, e: any) {
    if (!newDate) return;
    const resultState = await fetchCommitBeforeTime(
      state.reference,
      newDate,
      stateKey,
      apiV2
    );
    if (resultState) {
      setRefState(resultState as any);
      // Picker will be replaced in DX-53586
      if (!e) hidePicker(); // Workaround to close picker after time select: event is undefined when clicking time
    }
  }

  const firstCommit = useRef<LogEntry>();
  function commitListLoaded(data: LogResponse | undefined) {
    firstCommit.current = data?.logEntries?.[0];
  }

  useEffect(() => {
    if (!popupState.isOpen) return;
    setFormVal(state.hash ? RADIO_COMMIT : RADIO_HEAD);
  }, [popupState.isOpen, state.hash]);

  const { reference, hash, date } = refState;
  if (!state.reference) return null; //Loading

  return (
    <div
      onClick={(e) => {
        e.stopPropagation();
        e.preventDefault();
      }}
      onContextMenu={(e) => {
        e.stopPropagation();
      }}
    >
      <div
        className="branchPicker"
        {...toggleProps}
        onClick={(e) => {
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
        <ClickAwayListener onClickAway={cancel}>
          <Popover
            {...bindPopper(popupState)}
            transitionDuration={200}
            {...position}
            {...(getAnchorEl && { anchorEl: getAnchorEl })}
            onClose={cancel}
          >
            <div
              className="branchPicker-popup"
              onClick={(e) => {
                e.stopPropagation();
              }}
            >
              <DialogContent
                actions={
                  <>
                    <Button variant="secondary" onClick={cancel}>
                      <FormattedMessage id="Common.Cancel" />
                    </Button>
                    <Button variant="primary" onClick={submitForm}>
                      <FormattedMessage id="Common.Apply" />
                    </Button>
                  </>
                }
                title={
                  state.hash ? getShortHash(state.hash) : state.reference.name
                }
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
                                  onDataChange={commitListLoaded}
                                  disabled={formVal !== "commit"}
                                  key={reference.name}
                                  branch={reference}
                                  onClick={setHash}
                                  selectedHash={hash}
                                  api={apiV2}
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
        </ClickAwayListener>
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
