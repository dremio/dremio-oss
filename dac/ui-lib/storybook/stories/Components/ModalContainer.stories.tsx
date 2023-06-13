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

import { Meta, StoryFn } from "@storybook/react";

import {
  Button,
  DialogContent,
  ModalContainer,
  useModalContainer,
} from "../../../components";

export default {
  title: "Components/ModalContainer",
  component: ModalContainer,
} as Meta<typeof ModalContainer>;

export const Default: StoryFn<typeof ModalContainer> = () => {
  const myModal = useModalContainer();
  return (
    <div>
      <Button variant="primary" onClick={myModal.open}>
        Open modal container
      </Button>
      <ModalContainer {...myModal}>
        <DialogContent
          title="Sample Dialog"
          actions={
            <Button variant="secondary" onClick={myModal.close}>
              Close
            </Button>
          }
        >
          <div className="dremio-prose">
            <p>
              {" "}
              <strong>Lorem ipsum dolor sit amet,</strong> consectetur
              adipiscing elit. Nam neque ante, porttitor vel convallis in,
              ullamcorper sed arcu. In ultrices magna nec auctor feugiat.
              Quisque aliquam, nulla et scelerisque condimentum, magna quam
              condimentum erat, non ultrices est arcu in lorem. Vivamus nec mi
              auctor, ornare dolor vitae, feugiat mi. Nam sodales metus sed
              tortor iaculis, quis convallis tellus ornare. Phasellus ac
              faucibus arcu. Suspendisse nec ipsum augue. Nullam tempus tellus a
              enim luctus luctus. Vestibulum eu nibh et velit varius tincidunt
              quis vitae lectus. Cras cursus turpis arcu, quis facilisis sem
              eleifend ac. Suspendisse aliquet, lacus eu auctor pellentesque,
              lorem odio venenatis tortor, quis mollis libero ipsum vitae massa.
              Sed ullamcorper imperdiet felis, id dignissim nunc elementum ut.
              Pellentesque tincidunt felis vitae pulvinar varius. Nunc a erat
              congue orci tristique malesuada. Nullam dictum facilisis pretium.
              Duis ligula mauris, aliquam ut tortor elementum, euismod euismod
              mi.
            </p>
            <p>
              Sed nec dui magna. Donec ultricies feugiat est. Duis finibus nunc
              lectus, at placerat purus venenatis vitae. Fusce eu purus in ante
              elementum interdum. Proin posuere non erat a rutrum. Nullam auctor
              tortor a hendrerit consequat. Duis tempor volutpat luctus. Nam
              ipsum lorem, ornare sit amet justo quis, pharetra dapibus nulla.
            </p>
            <p>
              Sed feugiat justo a placerat vulputate. Class aptent taciti
              sociosqu ad litora torquent per conubia nostra, per inceptos
              himenaeos. Sed at auctor arcu. Curabitur quis condimentum justo.
              Nulla eu vehicula nibh. Praesent vel congue dolor. Aenean mattis
              erat et arcu vehicula, id interdum nulla cursus. Donec efficitur
              ultricies mi quis vestibulum. Aenean tincidunt tellus sit amet
              urna hendrerit condimentum. Aenean metus tellus, efficitur
              sagittis volutpat at, imperdiet vitae nisi. Cras dignissim lacus
              lectus, a pellentesque tellus varius sed. Nam ligula turpis,
              porttitor eu mollis et, tristique ut tellus. Mauris convallis
              libero sed ligula malesuada convallis.
            </p>
            <p>
              Mauris ut ultrices risus. Nullam tincidunt ex eget ligula rhoncus,
              sit amet tempus lectus pellentesque. Donec consequat porta orci,
              sed cursus justo suscipit quis. Donec risus ligula, sodales sit
              amet vehicula non, bibendum et justo. Mauris scelerisque
              ullamcorper pretium. Nunc ipsum ex, varius eu ullamcorper ut,
              molestie a lacus. Nunc id porta nulla, non rutrum massa. Maecenas
              vel ullamcorper neque. In et enim sagittis, laoreet augue eget,
              maximus sapien. Maecenas sed ipsum sollicitudin, dapibus nisi
              tristique, imperdiet nisl. Proin pulvinar tincidunt odio, eu
              posuere nulla congue non.
            </p>
            <p>
              Vivamus non aliquet neque. Ut dignissim ligula felis, dapibus
              molestie nulla lacinia ac. Praesent id vehicula metus. Duis
              posuere convallis erat, eget euismod turpis imperdiet eget. Cras
              pharetra rutrum risus eget sollicitudin. Integer ornare sit amet
              risus et facilisis. Morbi eu mollis erat, ac dictum nibh.
              Phasellus nisl libero, lacinia eget consequat vel, rutrum eu leo.
              Class aptent taciti sociosqu ad litora torquent per conubia
              nostra, per inceptos himenaeos. Pellentesque condimentum aliquet
              bibendum.
            </p>
          </div>
        </DialogContent>
      </ModalContainer>
    </div>
  );
};

Default.storyName = "ModalContainer";
