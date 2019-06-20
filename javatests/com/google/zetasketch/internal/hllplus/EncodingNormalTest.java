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

package com.google.zetasketch.internal.hllplus;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertEquals;

import com.google.zetasketch.internal.hllplus.Encoding.Normal;
import com.google.zetasketch.testing.IntegerBitSubject;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Encoding.Sparse}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class EncodingNormalTest {

  @Test
  public void index() {
    Normal encoding = new Normal(5);
    IntegerBitSubject.assertThat(encoding.index(0b101110001L << 55)).isEqualTo(0b10111);
  }

  @Test
  public void rhoW() {
    Normal encoding = new Normal(5);
    // Number of leading zero bits after the index (= the first 5 bits) is 3, rhoW is 3 + 1 = 4.
    assertEquals(4, encoding.rhoW(0b101110001L << 55));
  }

  @Test
  public void downgradeIndex() {
    Normal source = new Normal(5);
    Normal target = new Normal(3);
    IntegerBitSubject.assertThat(source.downgradeIndex(0b10111, target)).isEqualTo(0b101);
  }

  @Test
  public void downgradeRhoW_none() {
    Normal source = new Normal(5);
    Normal target = new Normal(3);
    // 0 indicates no value, should be kept as 0.
    assertThat(source.downgradeRhoW(0b10001, (byte) 0, target)).isEqualTo(0);
  }

  @Test
  public void downgradeRhoW_nonZero() {
    Normal source = new Normal(5);
    Normal target = new Normal(3);
    // Number of leading zero bits after the new index (= the first 3 bits) is 1, rhoW is 1 + 1 = 2.
    assertThat(source.downgradeRhoW(0b10001, (byte) 4, target)).isEqualTo(2);
  }

  @Test
  public void downgradeRhoW_zero() {
    Normal source = new Normal(5);
    Normal target = new Normal(3);
    // Number of leading zero bits after the new index is known (since all zeros) so new rhoW is
    // the old rhoW + 5 - 3 = 6.
    assertThat(source.downgradeRhoW(0b10000, (byte) 4, target)).isEqualTo(6);
  }
}
