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

import * as PATHS from "../../../paths";
import { useSonarProjects } from "../../../providers/useSonarProjects";
import {
  Button,
  ModalContainer,
  useModalContainer,
} from "dremio-ui-lib/dist-esm";
import { Link } from "react-router";
import classes from "./ServicesSection.less";

const newOnboardingEnabled = false;

export const SonarCardAction = () => {
  const sonarQuickstartModal = useModalContainer();
  const [sonarProjects] = useSonarProjects();

  let action = (
    <Link
      to={PATHS.sonarProjects()}
      className={classes["services-section__catalog-link"]}
    >
      <dremio-icon name="interface/arrow-right"></dremio-icon> See all Sonar
      Projects
    </Link>
  );

  if (newOnboardingEnabled && sonarProjects?.length === 0) {
    action = (
      <Button
        variant="primary"
        style={{ marginInline: "auto" }}
        onClick={sonarQuickstartModal.open}
      >
        <dremio-icon name="interface/magic" alt=""></dremio-icon> Quickstart to
        add Sonar Project
      </Button>
    );
  }

  return (
    <>
      <ModalContainer {...sonarQuickstartModal}>
        Sonar Quickstart
      </ModalContainer>
      {action}
    </>
  );
};
