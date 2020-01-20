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

import com.google.zetasketch.IncompatiblePrecisionException;
import it.unimi.dsi.fastutil.ints.AbstractIntIterator;
import it.unimi.dsi.fastutil.ints.IntIterator;
import java.util.NoSuchElementException;
import java.util.Objects;
import javax.annotation.Nullable;

/**
 * Encapsulates the encoding and decoding of singular HyperLogLog++ values. In particular, this
 * class implements:
 *
 * <ul>
 *   <li>Retrieval of HyperLogLog++ properties such as the index and the <em>&rho;(w)</em> of a
 *       uniform hash of the input value.
 *   <li>Encoding and decoding of HyperLogLog++ sparse values.
 * </ul>
 */
public final class Encoding {

  /**
   * An object that computes HyperLogLog++ properties for the normal encoding at a given precision.
   */
  public static class Normal {
    public final int precision;

    public Normal(int precision) {
      assert 1 <= precision && precision <= 63
          : "valid index and rhoW can only be determined for precisions in the range [1, 63]";
      this.precision = precision;
    }

    /**
     * Returns the HyperLogLog++ index for the given hash. For example, for precision 5 and a 64 bit
     * hash such as
     *
     * <pre>{@code
     * 0110 1001 0110 1011 ...
     * }</pre>
     *
     * the index is the first five bits {@code 01101}.
     */
    public int index(long hash) {
      return (int) (hash >>> (64 - precision));
    }

    /**
     * Returns the HyperLogLog++ <em>&rho;(w)</em> for the given hash, which is the number of
     * leading zero bits + 1 for the bits after the normal index. For example, for precision 5 and a
     * 64 bit hash such as
     *
     * <pre>{@code
     * 0110 1001 0110 1011 ...
     * }</pre>
     *
     * the part relevant for the <em>&rho;(w)</em> begins with {@code 001 0110 1011 ...} which has
     * two leading zeros. The <em>&rho;(w)</em> value will therefore be 3.
     */
    public byte rhoW(long hash) {
      return computeRhoW(hash, 64 - precision);
    }

    /**
     * Downgrades an index for the given target encoding.
     *
     * <p><b>Important:</b> Since this method is in the critical path of the library, callers are
     * responsible for verifying that the target precision &lt; the receiver’s precision. In all
     * other cases the behavior is undefined.
     */
    public int downgradeIndex(int index, Normal target) {
      assert target.precision <= precision;
      return index >> (precision - target.precision);
    }

    /**
     * Downgrades a <i>&rho;(w)</i> for the given target encoding. The index is required as the new
     * <i>&rho;(w)</i> will be computed from it.
     *
     * <p><b>Important:</b> Since this method is in the critical path of the library, callers are
     * responsible for verifying that the target precision is less than the receiver’s precision. In
     * all other cases the behavior is undefined.
     */
    public byte downgradeRhoW(int index, byte rhoW, Normal target) {
      // Preserve 0 rhoW in the normal encoding since this represents any unset register.
      if (rhoW == 0) {
        return 0;
      }

      return Encoding.downgradeRhoW(index, rhoW, this.precision, target.precision);
    }
  }

  /**
   * An object that computes HyperLogLog++ properties for the sparse encoding at a given precision.
   *
   * <p>Sparse values take one of two different representations depending on whether the normal
   * <em>&rho;(w)</em> can be determined from the lowest <em>sp-p</em> bits of the sparse index or
   * not. We use an (appropriately 0 padded) flag to indicate when the encoding includes an explicit
   * sparse <em>&rho;(w')</em>:
   *
   * <pre>
   *   +---+-------------------+-----------------------------+
   *   | 0 |      padding      |        sparse index         |
   *   +---+-------------------+-----------------------------+
   *        max(0, p+6-sp) bits           sp bits
   *
   *
   *   +---+-------------------+-------------------+---------+
   *   | 1 |      padding      |    normal index   |  rhoW'  |
   *   +---+-------------------+-------------------+---------+
   *        max(0, sp-p-6) bits       p bits         6 bits
   * </pre>
   *
   * <p>Note the subtle difference in nomenclature between <em>&rho;(w)</em> for the number of
   * leading zero bits + 1 relative to the <em>normal</em> precision and <em>&rho;(w')</em> for the
   * number of leading zero bits + 1 relative to the <em>sparse</em> precision. See the HLL++ paper
   * (https://goo.gl/pc916Z) for details.
   */
  public static class Sparse {
    /**
     * The number of bits used to encode the sparse <em>&rho;(w')</em> in the &rho;-encoded form.
     */
    private static final int RHOW_BITS = 6;

    /** Mask for isolating the <em>&rho;(w')</em> value in a &rho;-encoded sparse value. */
    private static final int RHOW_MASK = (1 << RHOW_BITS) - 1;

    public final int normalPrecision;
    public final int sparsePrecision;

    /**
     * Flag used to indicate whether a particular value is <em>&rho;(w')</em> encoded or not. The
     * position of the flag depends on the normal and sparse precisions. We store it here to avoid
     * having to recompute it every time we encode or decode a value.
     */
    private final int rhoEncodedFlag;

    private final Normal normal;

    public Sparse(int normalPrecision, int sparsePrecision) {
      // We want the sparse values to be sorted consistently independent of whether an
      // implementation uses signed or unsigned integers. The upper limit for the normal precision
      // is therefore 31 - RHOW_BITS - 1 (for flag).
      assert 1 <= normalPrecision && normalPrecision <= 24
          : "normal precision must be between 1 and 24 (inclusive)";
      // While for the sparse precision it is 31 - 1 (for flag).
      assert 1 <= sparsePrecision && sparsePrecision <= 30
          : "sparse precision must be between 1 and 30 (inclusive)";
      assert sparsePrecision >= normalPrecision
          : "sparse precision must be larger than or equal to the normal precision";

      this.normalPrecision = normalPrecision;
      this.sparsePrecision = sparsePrecision;

      // The position of the flag needs to be larger than any bits that could be used in the rhoW or
      // non-rhoW encoded values so that (a) the two values can be distinguished and (b) they will
      // not interleave when sorted numerically.
      rhoEncodedFlag = 1 << Math.max(sparsePrecision, normalPrecision + RHOW_BITS);

      this.normal = new Normal(normalPrecision);
    }

    /**
     * Checks whether a sparse encoding is compatible with another. This encoding's precision and
     * sparsePrecision must be both smaller or equal, or both bigger or equal to the {@code other}
     * Encoding's precisions for them to be compatible.
     */
    public void assertCompatible(Sparse other) {
      if ((this.normalPrecision <= other.normalPrecision
              && this.sparsePrecision <= other.sparsePrecision)
          || (this.normalPrecision >= other.normalPrecision
              && this.sparsePrecision >= other.sparsePrecision)) {
        return;
      }
      throw new IncompatiblePrecisionException(
          String.format(
              "Precisions (p=%d, sp=%d) are not compatible to (p=%d, sp=%d)",
              this.normalPrecision,
              this.sparsePrecision,
              other.normalPrecision,
              other.sparsePrecision));
    }

    /**
     * Returns the sparse encoding of the given hash value. The returned encoding can take one of
     * two formats depending on whether the normal <em>&rho;(w)</em> can be determined from the
     * sparse index or not. See the class Javadoc for details.
     */
    public int encode(long hash) {
      int sparseIndex = (int) (hash >>> (64 - sparsePrecision));
      byte sparseRhoW = computeRhoW(hash, 64 - sparsePrecision);

      int sparseValue = encode(sparseIndex, sparseRhoW);
      assert sparseValue >= 0
          : "post-condition failed: sparse value should always be a positive integer so that it is "
              + "sorted consistently across platforms";
      return sparseValue;
    }

    public int encode(int sparseIndex, byte sparseRhoW) {
      // Only check values in debug mode for efficiency.
      assert sparseIndex <= (1 << sparsePrecision) - 1;
      assert sparseRhoW <= (1 << RHOW_BITS) - 1;

      // Check if the normal rhoW can be re-constructed from the lowest sp-p bits of the sparse
      // index. In that case, we do not need to encode it explicitly.
      int mask = (1 << (sparsePrecision - normalPrecision)) - 1;
      if ((sparseIndex & mask) != 0) {
        return sparseIndex;
      }

      // Use the normal index instead of the sparse index since the lowest sp-p bits are all 0
      // anyway (see the mask above).
      //
      int normalIndex = sparseIndex >> (sparsePrecision - normalPrecision);
      return rhoEncodedFlag | normalIndex << RHOW_BITS | sparseRhoW;
    }

    /**
     * Decodes the sparse index from an encoded sparse value. See the class Javadoc for details on
     * the two representations with which sparse values are encoded.
     */
    public int decodeSparseIndex(int sparseValue) {
      // If the sparse rhoW' is not encoded, then the value consists of just the sparse index.
      if ((sparseValue & rhoEncodedFlag) == 0) {
        return sparseValue;
      }

      // When the sparse rhoW' is encoded, this indicates that the last sp-p bits of the sparse
      // index were all zero. We return the normal index right zero padded by sp-p bits since the
      // sparse index is just the normal index without the trailing zeros.
      return ((sparseValue ^ rhoEncodedFlag) // Strip the encoding flag.
              >> RHOW_BITS) // Strip the rhoW'
          << (sparsePrecision - normalPrecision); // Shift the normal index to sparse index length.
    }

    /**
     * Returns the sparse <em>&rho;(w')</em> from an encoded sparse value. See the class Javadoc for
     * details on the two representations with which sparse values are encoded.
     *
     * <p>Returns 0 if the value does not encode a sparse <em>&rho;(w')</em>.
     */
    public byte decodeSparseRhoWIfPresent(int sparseValue) {
      // Return 0 (arbitrarily chosen) if the sparse rhoW' is not encoded. In this case, the rhoW'
      // is ignored by all callers of this method, as the normal rhoW is determined by the last sp-p
      // bits of the sparse index alone.
      if ((sparseValue & rhoEncodedFlag) == 0) {
        return 0;
      }

      // In rhoW' encoded values, the last MAX_RHO_BITS contain the sparse rhoW' value.
      return (byte) (sparseValue & RHOW_MASK);
    }

    /**
     * Decodes the normal index from an encoded sparse value. See the class Javadoc for details on
     * the two representations with which sparse values are encoded.
     */
    public int decodeNormalIndex(int sparseValue) {
      // Values without a sparse rhoW' consist of just the sparse index, so the normal index is
      // determined by stripping off the last sp-p bits.
      if ((sparseValue & rhoEncodedFlag) == 0) {
        return sparseValue >> (sparsePrecision - normalPrecision);
      }

      // Sparse rhoW' encoded values contain a normal index so we extract it by stripping the flag
      // off the front and the rhoW' off the end.
      return (sparseValue ^ rhoEncodedFlag) >> RHOW_BITS;
    }

    /**
     * Decodes the normal <em>&rho;(w)</em> from an encoded sparse value. See the class Javadoc for
     * details on the two representations with which sparse values are encoded.
     */
    public byte decodeNormalRhoW(int sparseValue) {
      // If the rhoW' was not encoded, we can determine the normal rhoW from the last sp-p bits of
      // the sparse index.
      if ((sparseValue & rhoEncodedFlag) == 0) {
        return computeRhoW(sparseValue, sparsePrecision - normalPrecision);
      }

      // If the sparse rhoW' was encoded, this tells us that the last sp-p bits of the
      // sparse index where all zero. The normal rhoW is therefore rhoW' + sp - p.
      return (byte) ((sparseValue & RHOW_MASK) + sparsePrecision - normalPrecision);
    }

    /**
     * Decodes the normal index from an encoded sparse value, downgrading it to the given target
     * encoding if needed.
     *
     * <p>See the class Javadoc for details on the two representations with which sparse values are
     * encoded.
     */
    public int decodeAndDowngradeNormalIndex(int sparseValue, Normal target) {
      return decodeNormalIndex(sparseValue) >> (normalPrecision - target.precision);
    }

    /**
     * Decodes the normal <i>&rho;(w)</i> from an encoded sparse value, dowgrading it to the given
     * target encoding if needed.
     *
     * <p>See the class Javadoc for details on the two representations with which sparse values are
     * encoded.
     */
    public byte decodeAndDowngradeNormalRhoW(int sparseValue, Normal target) {
      return downgradeRhoW(
          decodeNormalIndex(sparseValue),
          decodeNormalRhoW(sparseValue),
          this.normalPrecision,
          target.precision);
    }

    /** Re-encodes a sparse value to match a target sparse encoding. */
    public int downgrade(int sparseValue, Sparse target) {
      int oldSparseIndex = decodeSparseIndex(sparseValue);
      byte oldSparseRhoW = decodeSparseRhoWIfPresent(sparseValue);

      // If sparseValue is not rhoW-encoded then downgradeRhoW will compute the rhoW from the index.
      int newSparseIndex = oldSparseIndex >> (sparsePrecision - target.sparsePrecision);
      byte newSparseRhoW =
          downgradeRhoW(oldSparseIndex, oldSparseRhoW, sparsePrecision, target.sparsePrecision);
      return target.encode(newSparseIndex, newSparseRhoW);
    }

    public IntIterator downgrade(final IntIterator iter, Sparse target) {
      return new AbstractIntIterator() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public int nextInt() {
          return downgrade(iter.next(), target);
        }
      };
    }

    /**
     * Takes a sorted iterator of sparse values and returns an iterator with deduplicated indices,
     * returning only the one with the largest <em>&rho;(w')</em>. For example, a list of sparse
     * values with <em>p=4</em> and <em>sp=7</em> such as:
     *
     * <pre>{@code
     * 0 000 0010100
     * 0 000 1010100
     * 0 000 1010101
     * 1 1010 001100
     * 1 1010 010000
     * 1 1110 000000
     * }</pre>
     *
     * Will be deduplicated to
     *
     * <pre>{@code
     * 0 000 0010100
     * 0 000 1010100
     * 0 000 1010101
     * 1 1010 010000
     * 1 1110 000000
     * }</pre>
     */
    public IntIterator dedupe(final IntIterator sortedValues) {
      return new AbstractIntIterator() {
        private boolean hasNext;
        private int next;

        // Member block to initialize #hasNext and #next so that we avoid having to write the same
        // hasNext = ...; if (hasNext) { next = ... } incantation multiple times.
        {
          advance();
        }

        @Override
        public boolean hasNext() {
          return hasNext;
        }

        private void advance() {
          hasNext = sortedValues.hasNext();
          if (hasNext) {
            next = sortedValues.nextInt();
          }
        }

        @Override
        public int nextInt() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }

          // Value is not rho-encoded so we don't need to do any special decoding as the value is
          // the sparse index. Simply skip exact duplicates.
          if ((next & rhoEncodedFlag) == 0) {
            int sparseIndex = next;
            do {
              advance();
            } while (hasNext && next == sparseIndex);
            return sparseIndex;
          }

          // Keep consuming values until we encounter one with a different index or run out of
          // values. We return the largest value (which will be the one with the largest rhoW).
          int sparseIndex = decodeSparseIndex(next);
          int maxSparseValue;
          do {
            maxSparseValue = next;
            advance();
          } while (hasNext && sparseIndex == decodeSparseIndex(next));

          return maxSparseValue;
        }
      };
    }

    /**
     * Defines a partial ordering of sparse precisions, returning true if either the normal or
     * sparse precision of the receiver is smaller than that of the argument.
     *
     * <p>Note that this doesn't verify that the encodings are actually compatible. For example,
     * {@code new Sparse(2, 6).isLessThan(new Sparse(3, 5))} will return {@code true}.
     */
    public boolean isLessThan(Sparse other) {
      return this.normalPrecision < other.normalPrecision
          || this.sparsePrecision < other.sparsePrecision;
    }

    @Override
    public boolean equals(@Nullable Object other) {
      if (!(other instanceof Sparse)) {
        return false;
      }

      Sparse o = (Sparse) other;
      return this.normalPrecision == o.normalPrecision && this.sparsePrecision == o.sparsePrecision;
    }

    @Override
    public int hashCode() {
      return Objects.hash(this.normalPrecision, this.sparsePrecision);
    }

    /** Returns a normal encoding corresponding to this sparse encoding. */
    public Normal normal() {
      return normal;
    }
  }

  /** Returns the number of leading zeros + 1 in the lower n {@code bits} of the value. */
  private static byte computeRhoW(long value, int bits) {
    // Strip of the index and move the rhoW to a higher order.
    long w = value << (64 - bits);

    // If the rhoW consists only of zeros, return the maximum length of bits + 1.
    return (w == 0)
        ? (byte) (bits + 1)
        : (byte) (Long.numberOfLeadingZeros(w) + 1);
  }

  /**
   * Computes a downgraded <i>&rho;(w)</i> (or <i>&rho;(w')</i>) in a target precision from an
   * existing index and <i>&rho;(w)</i> in a higher precision.
   *
   * <p><b>Important:</b> Since this method is in the critical path of the library, callers are
   * responsible for verifying that the target precision &lt; the receiver’s precision. In all other
   * cases the behavior is undefined.
   */
  private static byte downgradeRhoW(int index, byte rhoW, int sourceP, int targetP) {
    if (sourceP == targetP) {
      // Precision is the same, therefore rhoW is the same. Note that we can't calculate this as
      // below, because that would result in a left shift by 32, which is the same as a left shift
      // by 0 (i.e. returning the input, rather than returning 0).
      return rhoW;
    }

    assert targetP < sourceP;

    // Splice off the new index by bit shifting just past the index prefix. If the new suffix is
    // not all zeros, then the new rhoW is just the number of leading zeros + 1 in the new suffix.
    //
    // Otherwise, the old rhoW needs to be updated to account for the additional number of leading
    // zeros.

    int suffix = index << (Integer.SIZE - sourceP + targetP);

    if (suffix == 0) {
      return (byte) (rhoW + sourceP - targetP);
    }

    return (byte) (1 + Integer.numberOfLeadingZeros(suffix));
  }

  // Utility class.
  private Encoding() {}
}
