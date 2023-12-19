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
import { Button } from "dremio-ui-lib/components";
import { Popover } from "@mui/material";
import showTutorial from "./show-tutorial.png";

import * as classes from "./HideTutorialPopover.module.less";

export const HideTutorialPopover = (props: any) => {
  return (
    <Popover
      open={props.isOpen}
      transitionDuration={200}
      {...{
        anchorOrigin: { horizontal: "center", vertical: "center" },
        transformOrigin: { horizontal: "center", vertical: "center" },
      }}
    >
      <div className={classes["hide-tutorial-popover"]}>
        <p>
          To retrieve this getting started guide, go to <b>Help</b>, then{" "}
          <b>Dremio Tutorial</b>
        </p>
        <img
          src={showTutorial}
          alt="Re-enable tutorial in Help menu"
          width="317"
          height="160"
          style={{ borderRadius: 4 }}
        />
        <div className="flex justify-end">
          <Button variant="primary" onClick={props.close}>
            Got it!
          </Button>
        </div>
      </div>
    </Popover>
  );
};
