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

import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import java.util.NoSuchElementException;

/**
 * Iterator that takes two sorted integer iterators and merges them so that the integers are
 * returned in sorted order.
 */
public class MergedIntIterator extends AbstractIntIterator {

  private final IntIterator a;
  private final IntIterator b;

  private boolean aHasNext;
  private boolean bHasNext;
  private int aNext;
  private int bNext;

  public MergedIntIterator(IntIterator a, IntIterator b) {
    this.a = a;
    this.b = b;

    // Preload the first values so that we can compare them.
    advanceA();
    advanceB();
  }

  /** Advances iterator {@link #a}, returning the previous value. */
  private int advanceA() {
    int result = aNext;
    aHasNext = a.hasNext();
    if (aHasNext) {
      aNext = a.nextInt();
    }
    return result;
  }

  /** Advances iterator {@link #b}, returning the previous value. */
  private int advanceB() {
    int result = bNext;
    bHasNext = b.hasNext();
    if (bHasNext) {
      bNext = b.nextInt();
    }
    return result;
  }

  @Override
  public boolean hasNext() {
    return aHasNext || bHasNext;
  }

  @Override
  public int nextInt() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }

    if (bHasNext && (!aHasNext || bNext < aNext)) {
      return advanceB();
    }
    return advanceA();
  }
}
