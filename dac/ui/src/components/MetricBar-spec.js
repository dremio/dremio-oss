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
// import { render } from 'rtlUtils';
// import MetricBar from './MetricBar';

// describe('MetricBar', () => {
//   const minimalProps = {
//     progress: 0,
//     className: 'customClassname',
//     isProgressBar: false,
//     isLarge: false
//   };

//   it('should render with minimal props without exploding', () => {
//     const { getByTestId } = render(<MetricBar {...minimalProps} />);
//     expect(getByTestId('metricContainer')).to.exist;
//   });

//   it('should render with the metric container div tag', () => {
//     const { container } = render(<MetricBar {...minimalProps} />);
//     expect(container.querySelector('.progressBar')).to.not.exist;
//     expect(container.querySelector('.__metricBar')).to.exist;
//   });

//   it('should render with progress HTML tag', () => {
//     const props = {
//       ...minimalProps,
//       isProgressBar: true
//     };
//     const { container } = render(<MetricBar {...props} />);
//     expect(container.querySelector('.progressBar')).to.exist;
//     expect(container.querySelector('.__metricBar')).to.not.exist;
//   });
// });
