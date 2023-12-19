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
import React, { useEffect } from "react";
import Immutable from "immutable";
import { DialogContent, Drawer } from "dremio-ui-lib/components";
import WikiDrawerTitle from "@app/components/WikiDrawerTitle";
import WikiLanding from "@app/pages/ExplorePage/components/Wiki/WikiLanding";

interface WikiDrawerWrapperProps {
  datasetDetails: Immutable.Map<string, any> | undefined;
  fullPath: Immutable.List<string> | undefined;
  drawerIsOpen: boolean;
  closeWikiDrawer: (e: any) => void;
}

const WikiDrawerWrapper = ({
  datasetDetails,
  fullPath,
  drawerIsOpen,
  closeWikiDrawer,
}: WikiDrawerWrapperProps) => {
  useEffect(() => {
    // to stop stacking of drawers
    if (drawerIsOpen) {
      const existingDrawerLength =
        document.getElementsByClassName("wiki-drawer").length;
      const existingDrawer =
        existingDrawerLength > 1
          ? document.getElementsByClassName("wiki-drawer")[0]
          : null;
      if (existingDrawer) document.getElementById("close-wiki-drawer")?.click();
    }
  }, [drawerIsOpen]);
  return (
    <Drawer
      isOpen={drawerIsOpen}
      onClick={(e: React.MouseEvent<HTMLElement>) => {
        e.preventDefault();
        e.stopPropagation();
      }}
      className="wiki-drawer"
    >
      <DialogContent
        title={
          <WikiDrawerTitle
            datasetDetails={datasetDetails}
            fullPath={fullPath}
            closeWikiDrawer={closeWikiDrawer}
          />
        }
      >
        <WikiLanding datasetDetails={datasetDetails} />
      </DialogContent>
    </Drawer>
  );
};

export default WikiDrawerWrapper;
