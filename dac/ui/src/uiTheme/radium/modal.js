/*
 * Copyright (C) 2017-2018 Dremio Corporation
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

export const overlay = {
  backgroundColor: 'rgba(41,56,73,0.5)',
  width: '100%',
  height: '100%',
  pointerEvents: 'all'
};

export const modalContent = {
  width: '100%',
  border: 'none',
  borderRadius: 3,
  display: 'flex',
  flexDirection: 'column',
  margin: '0 auto',
  padding: 0,
  flexGrow: 1
};

export const smallModal = {
  overlay,
  content: {
    ...modalContent,
    overflow: 'visible',
    width: 670,
    height: 480,

    top: '50%',
    marginTop: -480 / 2
  }
};

export const smallestModal = {
  overlay,
  content: {
    ...modalContent,
    overflow: 'visible',
    width: 450,
    height: 200,

    top: '50%',
    marginTop: -200 / 2
  }
};

export const mediumModal = {
  overlay,
  content: {
    ...modalContent,
    width: 840,
    top: null,
    bottom: null
  }
};

export const largeModal = {
  overlay,
  content: {
    ...modalContent,
    width: '90%',
    top: null,
    bottom: null,
    maxWidth: 1200
  }
};

export const tallModal = {
  overlay,
  content: {
    ...modalContent,
    width: '50%',
    top: null,
    bottom: null,
    maxWidth: 440
  }
};

export const modalBody = {
  display: 'flex',
  flexDirection: 'column',
  flexGrow: 1,
  width: '100%',
  height: '100%',
  overflow: 'auto' // Seems to work but this is a hack to get around mixing flex and height: 100%. See DX-8853
};

export const modalFooter = {
  width: '100%',
  backgroundColor: '#f3f3f3',
  height: 48,
  padding: 10,
  textAlign: 'right',
  flexShrink: 0
};

export const modalPadding = {
  padding: '0 15px'
};

export const confirmBodyText = {
  ...modalPadding,
  fontSize: 14,
  display: 'flex',
  flexGrow: 1,
  flexDirection: 'column',
  justifyContent: 'center',
  alignItems: 'left'
};
