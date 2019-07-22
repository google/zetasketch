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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import com.google.zetasketch.IncompatiblePrecisionException;
import com.google.zetasketch.internal.hllplus.Encoding.Normal;
import com.google.zetasketch.internal.hllplus.Encoding.Sparse;
import com.google.zetasketch.testing.IntegerBitIterableSubject;
import com.google.zetasketch.testing.IntegerBitSubject;
import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.fastutil.ints.IntIterators;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Encoding.Sparse}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class EncodingSparseTest {

  @Test
  public void assertCompatible_matchingPrecisions() {
    Sparse a = new Sparse(6, 11);
    Sparse b = new Sparse(6, 11);
    a.assertCompatible(b);
  }

  @Test
  public void assertCompatible_downgradeNormalPrecision() {
    Sparse a = new Sparse(6, 11);
    Sparse b = new Sparse(7, 11);
    a.assertCompatible(b);
    b.assertCompatible(a);
  }

  @Test
  public void assertCompatible_downgradeSparsePrecision() {
    Sparse a = new Sparse(6, 11);
    Sparse b = new Sparse(6, 12);
    a.assertCompatible(b);
    b.assertCompatible(a);
  }

  @Test
  public void assertCompatible_incompatibleDowngrade() {
    Sparse a = new Sparse(7, 11);
    Sparse b = new Sparse(6, 12);

    IncompatiblePrecisionException e =
        assertThrows(IncompatiblePrecisionException.class, () -> a.assertCompatible(b));
    assertThat(e)
        .hasMessageThat()
        .contains("Precisions (p=7, sp=11) are not compatible to (p=6, sp=12)");

    e = assertThrows(IncompatiblePrecisionException.class, () -> b.assertCompatible(a));
    assertThat(e)
        .hasMessageThat()
        .contains("Precisions (p=6, sp=12) are not compatible to (p=7, sp=11)");
  }

  @Test
  public void decodeNormalIndex_whenNotRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // No leading flag, so normal index is just the highest 4 bits
    IntegerBitSubject.assertThat(encoding.decodeNormalIndex(0b1010100)).isEqualTo(0b1010);
  }

  @Test
  public void decodeNormalIndex_whenRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // Leading flag, next 4 bits are the normal index
    IntegerBitSubject.assertThat(encoding.decodeNormalIndex(0b11010001100)).isEqualTo(0b1010);
  }

  @Test
  public void decodeAndDowngradeNormalIndex_whenNotRhoEncoded() {
    Sparse source = new Sparse(4, 7);
    Normal target = new Normal(3);
    // No leading flag, so normal index is just the highest 3 bits
    IntegerBitSubject.assertThat(source.decodeAndDowngradeNormalIndex(0b1010100, target))
        .isEqualTo(0b101);
  }

  @Test
  public void decodeAndDowngradeNormalIndex_whenRhoEncoded() {
    Sparse source = new Sparse(4, 7);
    Normal target = new Normal(3);
    // Leading flag, next 3 bits are the normal index
    IntegerBitSubject.assertThat(source.decodeAndDowngradeNormalIndex(0b11010001100, target))
        .isEqualTo(0b101);
  }

  @Test
  public void decodeNormalRhoW_whenNotRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // No leading flag, normal rhoW determined by the last sp-p = 3 bits
    assertEquals(1, encoding.decodeNormalRhoW(0b1010100));
  }

  @Test
  public void decodeNormalRhoW_whenRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // Leading flag, normal rhoW' is the value of the last 6 bits + sp-p (3)
    assertEquals(0b1100 + 3, encoding.decodeNormalRhoW(0b11010001100));
  }

  @Test
  public void decodeAndDowngradeNormalRhoW_whenNotRhoEncoded() {
    Sparse source = new Sparse(4, 7);
    Normal target = new Normal(3);
    // No leading flag, normal rhoW determined by the last sp-p' = 2 bits
    assertEquals(2, source.decodeAndDowngradeNormalRhoW(0b1010100, target));
  }

  @Test
  public void decodeAndDowngradeNormalRhoW_whenRhoEncoded() {
    Sparse source = new Sparse(4, 7);
    Normal target = new Normal(3);
    // Leading flag, normal rhoW' is the value of the last 6 bits + sp-p' (3)
    assertEquals(0b1100 + 4, source.decodeAndDowngradeNormalRhoW(0b11010001100, target));
  }

  @Test
  public void decodeSparseIndex_whenNotRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    IntegerBitSubject.assertThat(encoding.decodeSparseIndex(0b1010100)).isEqualTo(0b1010100);
  }

  @Test
  public void decodeSparseIndex_whenRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // Leading flag, sparse index is the next 4 bits (normal index) plus 3 zeros
    IntegerBitSubject.assertThat(encoding.decodeSparseIndex(0b11010001100)).isEqualTo(0b1010000);
  }

  @Test
  public void decodeSparseRhoWIfPresent_whenNotRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // No leading flag, sparse rhoW' is unknown
    assertEquals(0, encoding.decodeSparseRhoWIfPresent(0b1010100));
  }

  @Test
  public void decodeSparseRhoWIfPresent_whenRhoEncoded() {
    Sparse encoding = new Sparse(4, 7);
    // Leading flag, sparse rhoW' is the last sp-p = 6 bits
    assertEquals(0b1100, encoding.decodeSparseRhoWIfPresent(0b11010001100));
  }

  @Test
  public void dedupe() {
    IntIterator input =
        IntIterators.wrap(
            new int[] {
              0b00000010100,
              0b00001010100,
              0b00001010101,
              0b11010001100,
              0b11010010000,
              0b11110000000
            });

    Sparse encoding = new Sparse(4, 7);
    IntegerBitIterableSubject.assertThat(encoding.dedupe(input))
        .isEqualTo(
            IntIterators.wrap(
                new int[] {
                  0b00000010100, 0b00001010100, 0b00001010101, 0b11010010000, 0b11110000000
                }));
  }

  @Test
  public void dedupe_exactDuplicates() {
    IntIterator input =
        IntIterators.wrap(
            new int[] {
              0b00000010100,
              0b00000010100,
              0b00000010100,
              0b11010001100,
              0b11010001100,
              0b11010001100
            });

    Sparse encoding = new Sparse(4, 7);
    IntegerBitIterableSubject.assertThat(encoding.dedupe(input))
        .isEqualTo(
            IntIterators.wrap(
                new int[] {
                  0b00000010100, 0b11010001100,
                }));
  }

  @Test
  public void encode_withoutRhoW() {
    Sparse encoding = new Sparse(4, 7);
    IntegerBitSubject.assertThat(encoding.encode(0b101100101L << 55)).isEqualTo(0b1011001);
  }

  @Test
  public void encode_withoutRhoWAtMaximumSparsePrecision() {
    Sparse encoding = new Sparse(4, 30);
    IntegerBitSubject.assertThat(encoding.encode(0b101100101L << 55)).isEqualTo(0b101100101 << 21);
  }

  @Test
  public void encode_withRhoWAtMaximumNormalPrecision() {
    Sparse encoding = new Sparse(24, 26);
    IntegerBitSubject.assertThat(encoding.encode(0b101L << 61))
        .isEqualTo(
            (1 << 30) /* flag */
                | (0b101 << 27) /* normal index */
                | 39 /* number of zero bits + 1 after the sparse index */);
  }

  @Test
  public void encode_withRhoWAtMinimumNormalPrecision() {
    Sparse encoding = new Sparse(1, 5);
    IntegerBitSubject.assertThat(encoding.encode(0b1L << 63))
        .isEqualTo(
            (1 << 7) /* flag */
                | (0b1 << 6) /* normal index */
                | 60 /* number of zero bits + 1 after the sparse index */);
  }

  /**
   * Tests the sparse encoding when <em>&rho;(w')</em> is encoded, making sure that the flag is
   * sufficiently to the left that it does not collide with the normal index.
   */
  @Test
  public void encode_withRhoWWhenLengthDominatedByNormalIndex() {
    Sparse encoding = new Sparse(4, 7);
    IntegerBitSubject.assertThat(encoding.encode(0b101100001L << 55))
        .isEqualTo(
            (1 << 10) /* flag */
                | (0b1011 << 6) /* normal index */
                | 2 /* number of zero bits + 1 after the sparse index */);
  }

  /**
   * Tests the sparse encoding when <em>&rho;(w')</em> is encoded, making sure that the flag is
   * sufficiently to the left that it does not collide with the sparse index.
   */
  @Test
  public void encode_withRhoWWhenLengthDominatedBySparseIndex() {
    Sparse encoding = new Sparse(2, 9);
    IntegerBitSubject.assertThat(encoding.encode(0b110000000001L << 52))
        .isEqualTo(
            (1 << 9) /* flag */
                | (0b11 << 6) /* normal index */
                | 3 /* number of zero bits + 1 after the sparse index */);
  }

  /**
   * Tests that the value is <em>&rho;(w')</em>-encoded when the sparse precision is equal to the
   * normal precision, since there are no sp-p bits from which a <em>&rho;(w)</em> could be
   * determined.
   */
  @Test
  public void encode_withRhoWWhenSparsePrecisionIsEqualToNormalPrecision() {
    Sparse encoding = new Sparse(3, 3);
    IntegerBitSubject.assertThat(encoding.encode(0b10111L << 59))
        .isEqualTo(
            (1 << 9) /* flag */
                | (0b101 << 6) /* normal index */
                | 1 /* number of zero bits + 1 after the sparse index */);
  }

  /**
   * Tests that a <em>&rho;(w')</em> encoded value is correctly downgraded to a
   * non-<em>&rho;(w')</em> encoded value. This can happen when the normal precision <em>p</em>
   * decreases relative to the sparse precision <em>sp</em>.
   */
  @Test
  public void downgrade_rhoWToNonRhoW() {
    Sparse source = new Sparse(3, 5);
    Sparse target = new Sparse(2, 5);

    IntegerBitSubject.assertThat(
            source.downgrade(
                (1 << 9) /* flag */ | (0b111 << 6) /* normal index */ | 2 /* rhoW' */, target))
        .isEqualTo(0b11100);
  }

  /**
   * Tests that a non-<em>&rho;(w')</em> encoded value is correctly downgraded to a
   * <em>&rho;(w')</em> encoded value. This can happen when the sparse precision <em>sp</em>
   * decreases relative to the normal precision <em>p</em>.
   */
  @Test
  public void downgrade_nonRhoWToRhoW() {
    Sparse source = new Sparse(3, 5);
    Sparse target = new Sparse(3, 4);

    IntegerBitSubject.assertThat(source.downgrade(0b11101, target))
        .isEqualTo((1 << 9) /* flag */ | (0b111 << 6) /* normal index */ | 1 /* rhoW' */);
  }

  @Test
  public void downgrade_iterator() {
    Encoding.Sparse source = new Encoding.Sparse(11, 15);
    Encoding.Sparse target = new Encoding.Sparse(10, 13);

    IntIterator iter = IntIterators.wrap(new int[] {0b000000000000001, 0b000000000011111});

    // Preserves ordering of input values rather than sorting values.
    IntegerBitIterableSubject.assertThat(source.downgrade(iter, target))
        .isEqualTo(IntIterators.wrap(new int[] {0b10000000000000010, 0b000000000111}));
  }

  @Test
  public void normal() {
    Sparse sparse = new Sparse(3, 5);
    assertThat(sparse.normal().precision).isEqualTo(3);
  }

  @Test
  public void isLessThan() {
    assertTrue(new Sparse(3, 5).isLessThan(new Sparse(4, 5)));
    assertTrue(new Sparse(4, 5).isLessThan(new Sparse(4, 6)));
    assertTrue(new Sparse(3, 5).isLessThan(new Sparse(4, 6)));
    assertFalse(new Sparse(3, 5).isLessThan(new Sparse(3, 5)));

    // Currently doesn't verify that precisions are incompatible.
    assertTrue(new Sparse(2, 6).isLessThan(new Sparse(3, 5)));
  }

  @Test
  public void equalsTo() {
    assertThat(new Sparse(4, 5)).isEqualTo(new Sparse(4, 5));

    assertThat(new Sparse(4, 5)).isNotEqualTo(new Sparse(3, 5));
    assertThat(new Sparse(4, 5)).isNotEqualTo(new Sparse(4, 6));
    assertThat(new Sparse(4, 5)).isNotEqualTo(new Object());
    assertThat(new Sparse(4, 5)).isNotEqualTo(null);
  }
}
