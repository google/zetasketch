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

import static org.junit.Assert.assertEquals;

import com.google.common.collect.Lists;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import java.util.Arrays;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link MergedIntIterator}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class MergedIntIteratorTest {

  @Test
  public void next_ReturnsValuesInSortedOrder() {
    IntIterator a = IntIterators.wrap(new int[] {1, 2, 4});
    IntIterator b = IntIterators.wrap(new int[] {2, 3, 4, 5, 6, 7});

    assertEquals(
        Arrays.asList(1, 2, 2, 3, 4, 4, 5, 6, 7), Lists.newArrayList(new MergedIntIterator(a, b)));
  }
}
