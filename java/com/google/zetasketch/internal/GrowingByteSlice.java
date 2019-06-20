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
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A self-growing {@link ByteSlice}.
 *
 * <p>This is essentially the same as a {@link ByteSlice} with the difference that the limit is
 * automatically extended on {@code put}* operations. If the limit grows past the capacity, the
 * {@code GrowingByteSlice} will allocate a new backing array with a larger capacity.
 */
public final class GrowingByteSlice extends ByteSlice {

  /**
   * Factor by which to grow the byte buffer each time more capacity is needed. Having a value
   * greater than 1 has the advantage that the byte array does not have to be copied each time a
   * value is added to it.
   */
  private static final float GROWTH_FACTOR = 1.9f;

  /**
   * Typical maximum array size <a
   * href="http://stackoverflow.com/questions/3038392/do-java-arrays-have-a-maximum-size">according
   * to stackoverflow</a>. After this size, we won't try to grow by {@link #GROWTH_FACTOR} but only
   * by the requested capacity (which may still run out of memory).
   */
  private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

  /**
   * Creates a new byte slice. The slice's position will be zero, its limit will be its capacity and
   * each of its elements will be initialized to zero. The backing array will be mutable and its
   * array offset will be zero.
   */
  public static GrowingByteSlice allocate(int capacity) {
    Preconditions.checkArgument(capacity >= 0);
    GrowingByteSlice slice = new GrowingByteSlice();
    slice.initForAllocate(capacity);
    return slice;
  }

  /**
   * Wraps a byte array into a new byte slice. The slice's position will be zero, its limit will be
   * {@code array.length}. The backing array will be the given array and its array offset will be
   * zero.
   *
   * <p>The byte slice will never modify the byte array itself. Instead, it will create a copy as
   * soon as the first modification takes place. However, external changes to the byte array will be
   * reflected on the byte slice's read methods for the duration it is aliasing the data.
   *
   * <p>This is equivalent to <code>{@link #copyOnWrite(byte[], int, int) alias}(data, 0,
   * data.length)</code>.
   */
  public static GrowingByteSlice copyOnWrite(byte[] array) {
    GrowingByteSlice slice = new GrowingByteSlice();
    slice.initForCopyOnWrite(array, 0, 0, array.length);
    return slice;
  }

  /**
   * Wraps a byte array into a new byte slice. The slice's position will be zero, its limit will be
   * {@code array.length}. The backing array will be the given array and its array offset will be
   * zero.
   *
   * <p>The byte slice will never modify the byte array itself. Instead, it will create a copy as
   * soon as the first modification takes place. However, external changes to the byte array will be
   * reflected on the byte slice's read methods for the duration it is aliasing the data.
   *
   * <p>This is equivalent to <code>{@link #copyOnWrite(byte[], int, int) alias}(data, 0,
   * data.length)</code>.
   */
  public static GrowingByteSlice copyOnWrite(byte[] array, int offset, int length) {
    int limit = offset + length;
    Preconditions.checkPositionIndexes(offset, limit, array.length);
    GrowingByteSlice slice = new GrowingByteSlice();
    slice.initForCopyOnWrite(array, 0, offset, limit);
    return slice;
  }

  /**
   * Wraps the backing array of a {@link ByteBuffer} into a new byte slice. The backing array, the
   * array offset, the position and the limit will be taken from the buffer.
   *
   * @throws UnsupportedOperationException if the buffer is not backed by an accessible array.
   */
  public static GrowingByteSlice copyOnWrite(ByteBuffer buffer) {
    GrowingByteSlice slice = new GrowingByteSlice();
    slice.initForCopyOnWrite(
        buffer.array(), buffer.arrayOffset(), buffer.position(), buffer.limit());
    return slice;
  }

  // Clients should use factory methods.
  private GrowingByteSlice() {}

  @Override
  public GrowingByteSlice clear() {
    super.clear();
    return this;
  }

  @Override
  public GrowingByteSlice ensureWriteable() {
    super.ensureWriteable();
    return this;
  }

  @Override
  public GrowingByteSlice flip() {
    super.flip();
    return this;
  }

  /**
   * Sets this slice's limit. If the new limit is larger than the current capacity, the backing
   * array will be grown to make space for the new limit. If the limit is reduced to a value smaller
   * than the current position, the position will be updated to the new limit.
   *
   * @throws IndexOutOfBoundsException if the new limit is smaller than 0.
   */
  @Override
  public GrowingByteSlice limit(int newLimit) {
    if (newLimit < 0) {
      throw new IndexOutOfBoundsException();
    }

    // Grow if limit is set past the capacity.
    maybeExtendLimit(newLimit);

    // Reduce the position if the new limit is smaller.
    limit = newLimit;
    if (position > newLimit) {
      position = newLimit;
    }

    return this;
  }

  /**
   * Sets this buffer's position. If the position is larger than the current limit, the limit will
   * be increased to the new position.
   *
   * @throws IndexOutOfBoundsException if the position is negative
   */
  @Override
  public GrowingByteSlice position(int newPosition) {
    maybeExtendLimit(newPosition);
    super.position(newPosition);
    return this;
  }

  /**
   * Absolute <em>put</em> method to set a value to the maximum of {@code b} or the current value.
   * If index is past the current limit, the limit will be increased.
   *
   * @throws IndexOutOfBoundsException if {@code index} is negative
   */
  @Override
  public GrowingByteSlice putMax(int index, byte b) {
    maybeExtendLimit(index + 1);
    super.putMax(index, b);
    return this;
  }

  @Override
  public GrowingByteSlice putMax(int index, ByteSlice src) {
    maybeExtendLimit(index + src.remaining());
    super.putMax(index, src);
    return this;
  }

  @Override
  public GrowingByteSlice putNextVarInt(int value) {
    maybeExtendLimit(position + VarInt.varIntSize(value));
    ensureWriteable();
    position = uncheckedPutVarInt(position, value);
    return this;
  }

  @Override
  public GrowingByteSlice putVarInt(int index, int value) {
    maybeExtendLimit(index + VarInt.varIntSize(value));
    ensureWriteable();
    uncheckedPutVarInt(index, value);
    return this;
  }

  private void maybeExtendLimit(int newLimit) {
    if (newLimit <= limit) {
      return;
    }

    limit = newLimit;

    // Increase the capacity if necessary, growing beyond the limit by a growth factor. This may
    // over-allocate some memory but makes building a buffer of size N with multiple put operations
    // amortized O(N) instead of O(N^2).
    if (limit + arrayOffset > array.length) {
      int currentCapacity = array.length - arrayOffset;
      int growthCapacity = Math.min(MAX_ARRAY_SIZE, (int) (currentCapacity * GROWTH_FACTOR));

      array = Arrays.copyOfRange(array, arrayOffset(), Math.max(growthCapacity, limit));
      arrayOffset = 0;
      copyOnWrite = false;
    }
  }
}
