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

import static com.google.zetasketch.testing.VarIntSequence.varints;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import it.unimi.dsi.fastutil.ints.IntIterator;
import java.util.NoSuchElementException;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link DifferenceDecoder}. */
@RunWith(JUnit4.class)
public class DifferenceDecoderTest {

  @Test
  public void create_ThrowsWhenNull() {
    assertThrows(
        NullPointerException.class,
        () -> new DifferenceDecoder(null));
  }

  @Test
  public void hasNext_ReturnsFalseWhenEmpty() {
    IntIterator iter = new DifferenceDecoder(GrowingByteSlice.allocate(0));
    assertFalse(iter.hasNext());
  }

  @Test
  public void next_DecodesIntegers() {
    IntIterator iter = new DifferenceDecoder(GrowingByteSlice.copyOnWrite(varints(
        42,
        170 - 42,
        2903 - 170,
        20160531 - 2903
    )));

    assertTrue(iter.hasNext());
    assertEquals(42, iter.nextInt());

    assertTrue(iter.hasNext());
    assertEquals(170, iter.nextInt());

    assertTrue(iter.hasNext());
    assertEquals(2903, iter.nextInt());

    assertTrue(iter.hasNext());
    assertEquals(20160531, iter.nextInt());

    assertFalse(iter.hasNext());
  }

  @Test
  public void next_ThrowsWhenEmpty() {
    IntIterator iter = new DifferenceDecoder(GrowingByteSlice.allocate(0));

    assertThrows(
        NoSuchElementException.class,
        () -> iter.next());
  }
}
