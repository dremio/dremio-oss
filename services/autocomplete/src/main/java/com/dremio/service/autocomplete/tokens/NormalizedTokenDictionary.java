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
package com.dremio.service.autocomplete.tokens;

import java.util.HashMap;
import java.util.Map;

import com.dremio.exec.planner.sql.parser.impl.ParserImplConstants;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

/**
 * Dictionary for mapping token kinds to a normalized list of images.
 */
public final class NormalizedTokenDictionary {
  public static final NormalizedTokenDictionary INSTANCE = new NormalizedTokenDictionary();

  private final ImmutableList<String> images;
  private final ImmutableMap<String, Integer> imageToIndex;

  private NormalizedTokenDictionary() {
    ImmutableList.Builder<String> imagesBuilder = new ImmutableList.Builder<>();
    Map<String, Integer> imageToIndexBuilder = new HashMap<>();
    for (int index = 0; index < ParserImplConstants.tokenImage.length; index++) {
      String image = ParserImplConstants.tokenImage[index];
      String normalizedImage = image
        .toUpperCase()
        .replace("\"", "");

      imagesBuilder.add(normalizedImage);

      // It's possible that two tokens have the same image,
      // so just use the latest one.
      imageToIndexBuilder.putIfAbsent(image, index);
    }

    this.images = imagesBuilder.build();
    this.imageToIndex = ImmutableMap.copyOf(imageToIndexBuilder);
  }

  public String indexToImage(int index) {
    return images.get(index);
  }

  public int imageToIndex(String image) {
    return imageToIndex.get(image);
  }
}
