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

import static com.google.common.truth.Truth.assertThat;
import static com.google.zetasketch.testing.VarIntSequence.varints;
import static org.junit.Assert.assertThrows;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DifferenceEncoder}. */
@RunWith(JUnit4.class)
public class DifferenceEncoderTest {

  @Test
  public void putInt_CorrectlyWritesEqualElements() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);
    encoder.putInt(42);
    encoder.putInt(42);

    assertThat(slice.flip().toByteArray()).isEqualTo(varints(42, 42 - 42));
  }

  @Test
  public void putInt_CorrectlyWritesMultiple() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);

    encoder.putInt(42);
    encoder.putInt(170);
    encoder.putInt(2903);

    assertThat(slice.flip().toByteArray()).isEqualTo(varints(
        42,
        170 - 42,
        2903 - 170
    ));
  }

  @Test
  public void putInt_CorrectlyWritesSingle() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);
    encoder.putInt(42);

    assertThat(slice.flip().toByteArray()).isEqualTo(varints(42));
  }

  @Test
  public void putInt_CorrectlyWritesZero() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);
    encoder.putInt(0);

    assertThat(slice.flip().toByteArray()).isEqualTo(varints(0));
  }

  @Test
  public void putInt_ThrowsWhenNegative() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> encoder.putInt(-1));
    assertThat(e).hasMessageThat().isEqualTo("only positive integers supported but got -1");
  }

  @Test
  public void putInt_ThrowsWhenNotSorted() {
    GrowingByteSlice slice = GrowingByteSlice.allocate(0);
    DifferenceEncoder encoder = new DifferenceEncoder(slice);
    encoder.putInt(42);

    IllegalArgumentException e =
        assertThrows(IllegalArgumentException.class, () -> encoder.putInt(12));
    assertThat(e)
        .hasMessageThat()
        .isEqualTo("12 put after 42 but values are required to be in ascending order");
  }
}
