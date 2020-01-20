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
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import java.util.NoSuchElementException;

/**
 * A reader for difference encoded integers.
 *
 * <p>Difference encoding can efficiently store sorted integers by storing only the difference
 * between them, rather than their absolute values. Since the deltas between values should be small,
 * the representation additionally compacts them by using varint encoding.
 */
public class DifferenceDecoder extends AbstractIntIterator {

  private final ByteSlice slice;

  private int last = 0;

  public DifferenceDecoder(ByteSlice slice) {
    Preconditions.checkNotNull(slice);
    this.slice = slice;
  }

  @Override
  public boolean hasNext() {
    return slice.hasRemaining();
  }

  @Override
  public int nextInt() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    last += slice.getNextVarInt();
    return last;
  }
}
