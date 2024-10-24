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

import ApiUtils from "#oss/utils/apiUtils/apiUtils";
import { showUnsavedChangesConfirmDialog } from "#oss/actions/confirmation";
import { connect } from "react-redux";
import { Map, fromJS } from "immutable";
import { useEffect, useState } from "react";
import WikiModalView from "./WikiModal";
import { useIntl } from "react-intl";

type SaveProps = {
  saveVal: { text: string; version: string };
};

interface WikiModalWithSaveProps {
  entityId: string;
  entityType: string;
  fullPath: any;
  isOpen: boolean;
  wikiValue: string;
  wikiVersion: number;
  isReadMode: boolean;
  wikiSummary: boolean;
  // topSectionButtons: typeof SectionTitle.propTypes.buttons;
  onChange: () => void;
  save: (saveVal: SaveProps) => void;
  cancel: () => void;
}

const WikiModalWithSave = ({
  wikiVersion,
  entityId,
  entityType,
  fullPath,
  save,
  onChange,
  isOpen,
  wikiValue,
  wikiSummary,
  isReadMode = false,
  // topSectionButtons,
  cancel: cancelProp,
}: WikiModalWithSaveProps) => {
  const [wikiViewState, setWikiViewState] = useState<any>(new Map());
  const [wikiChanged, setWikiChanged] = useState<boolean>(false);
  const [wikiVal, setWikiVal] = useState<string>(wikiValue);
  const [openChildModal, setOpenChildModal] = useState<boolean>(false);
  const intl = useIntl();

  useEffect(() => {
    setWikiVal(wikiValue);
  }, [wikiValue]);

  const saveWiki = (newValue: string) => {
    return ApiUtils.fetch(
      `catalog/${entityId}/collaboration/wiki`,
      {
        method: "POST",
        body: JSON.stringify({
          text: newValue,
          version: wikiVersion,
        }),
      },
      3,
    ).then(
      (response: { json: () => Promise<SaveProps> }) => {
        setWikiChanged(false);
        return response.json().then(save, () => {}); // ignore json parsing error, but if save it not called, wiki will stay in edit mode
      },
      async (response: any) => {
        setWikiViewState(
          fromJS({
            isFailed: true,
            error: {
              message: await ApiUtils.getErrorMessage(
                intl.formatMessage({ id: "Wiki.NotSaved" }),
                response,
              ),
              id: "" + Math.random(),
            },
          }),
        );
      },
    );
  };
  const cancelHandler = () => {
    setWikiChanged(false);
    setWikiVal(wikiValue);
    setOpenChildModal(false);
    cancelProp();
  };

  const cancel = () => {
    // reset value to original wiki if not saved
    if (wikiChanged) {
      setOpenChildModal(true);
      return false;
    }
    cancelHandler();
  };

  const stay = () => {
    setOpenChildModal(false);
  };

  const leave = () => {
    cancelHandler();
  };

  const onChangeVal = (val: string) => {
    setWikiChanged(true);

    if (onChange) {
      onChange();
      setWikiVal(val);
    }
  };

  const props = {
    isOpen,
    wikiValue: wikiVal,
    isReadMode,
    entityId,
    entityType,
    fullPath,
    wikiSummary,
    // topSectionButtons,
    onChange: onChangeVal,
    save: saveWiki,
    cancel,
    wikiViewState,
    stay,
    leave,
    openChildModal,
  };

  return <WikiModalView {...props} />;
};

const mapDispatchToProps = {
  confirmUnsavedChanges: showUnsavedChangesConfirmDialog,
};
export default connect(null, mapDispatchToProps)(WikiModalWithSave);
