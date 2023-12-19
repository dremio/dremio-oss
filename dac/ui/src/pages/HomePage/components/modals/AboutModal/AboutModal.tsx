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

import { getIntlContext } from "dremio-ui-common/contexts/IntlContext.js";

import Modal from "@app/components/Modals/Modal";
import AboutModalLogoPane from "./AboutModalLogoPane";
import AboutModalInfoPane from "./AboutModalInfoPane";

import * as classes from "./AboutModal.module.less";

type AboutModalProps = {
  isOpen: boolean;
  hide: () => void;
};

const { t } = getIntlContext();

const isBeta = process.env.DREMIO_BETA === "true";

function AboutModal({ isOpen, hide }: AboutModalProps) {
  return (
    <Modal
      size="small"
      title={t("About.Dremio")}
      isOpen={isOpen}
      hide={hide}
      modalHeight={isBeta ? "560px" : "540px"}
    >
      <div className={classes["about-container"]}>
        <AboutModalLogoPane />
        <AboutModalInfoPane />
      </div>
    </Modal>
  );
}

export default AboutModal;
