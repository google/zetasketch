/*
 * Copyright 2019 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasketch.internal;

import com.google.common.base.Preconditions;

/**
 * A writer for difference encoded integers.
 *
 * <p>Difference encoding can efficiently store sorted integers by storing only the difference
 * between them, rather than their absolute values. Since the deltas between values should be small,
 * the representation additionally compacts them by using varint encoding.
 *
 * <p>The encoder only supports writing positive integers in ascending order.
 */
public class DifferenceEncoder {

  private final ByteSlice slice;
  private int last;

  /**
   * Creates a new DifferenceEncoder.
   *
   * @param slice to write the encoded values to
   */
  public DifferenceEncoder(ByteSlice slice) {
    this.slice = slice;
  }

  /**
   * Writes the integer value into the buffer using difference encoding.
   *
   * @throws IllegalArgumentException if the integer is negative or smaller than the previous
   *     encoded value.
   */
  public void putInt(int v) {
    Preconditions.checkArgument(v >= 0, "only positive integers supported but got %s", v);
    Preconditions.checkArgument(
        v >= last, "%s put after %s but values are required to be in ascending order", v, last);
    slice.putNextVarInt(v - last);
    last = v;
  }
}
