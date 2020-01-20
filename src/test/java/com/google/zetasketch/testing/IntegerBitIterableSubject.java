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

package com.google.zetasketch.testing;

import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.truth.FailureMetadata;
import com.google.common.truth.Subject;
import com.google.common.truth.Truth;
import java.util.Iterator;
import javax.annotation.Nullable;

/**
 * A truth subject that renders iterables of integers in binary rather than in decimal form. This is
 * useful as it can be hard to see what failed in a bit twiddling method from the decimal integer
 * representation (e.g. "[791] is not equal to [919]" vs. "[0b1100010111] is not equal to
 * [0b1110010111]").
 *
 * <p>The subject also adds support for integer iterators in addition to iterables. If an iterator
 * appears as subject or comparison object, it is consumed and internally converted to an iterable.
 */
public class IntegerBitIterableSubject extends Subject {

  private static final Subject.Factory<IntegerBitIterableSubject, Iterable<Integer>> FACTORY =
      IntegerBitIterableSubject::new;

  public static IntegerBitIterableSubject assertThat(Iterable<Integer> value) {
    return Truth.assertAbout(FACTORY).that(value);
  }

  public static IntegerBitIterableSubject assertThat(Iterator<Integer> value) {
    return Truth.assertAbout(FACTORY).that(Lists.newArrayList(value));
  }

  private final Iterable<Integer> actual;

  private IntegerBitIterableSubject(FailureMetadata failureMetadata, Iterable<Integer> subject) {
    super(failureMetadata, subject);
    this.actual = subject;
  }

  @Override
  public void isEqualTo(@Nullable Object other) {
    if (other instanceof Iterable<?>) {
      internalIsEqualTo((Iterable<?>) other);
    } else if (other instanceof Iterator<?>) {
      internalIsEqualTo(Lists.newArrayList((Iterator<?>) other));
    } else {
      failWithActual("expected to be equal to", other);
    }
  }

  private void internalIsEqualTo(Iterable<?> other) {
    if (!Iterables.elementsEqual(this.actual, other)) {
      this.failWithActual("expected to be equal to", toString(other));
    }
  }

  @Override
  protected String actualCustomStringRepresentation() {
    return toString(actual);
  }

  private static String toString(Iterable<?> list) {
    StringBuilder builder = new StringBuilder("[");
    boolean first = true;
    for (Object value : list) {
      if (!first) {
        builder.append(", ");
      }
      first = false;

      if (value instanceof Integer) {
        builder.append("0b").append(Integer.toBinaryString(((Integer) value).intValue()));
      } else {
        builder.append(value);
      }
    }
    builder.append("]");
    return builder.toString();
  }
}
