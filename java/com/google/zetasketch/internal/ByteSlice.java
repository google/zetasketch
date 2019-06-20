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

import static com.google.common.base.Preconditions.checkPositionIndex;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import java.nio.ByteBuffer;
import java.util.Arrays;

/**
 * A byte array wrapper that supports copy-on-write and fast reading (<em>get</em>*) and writing
 * methods (<em>put</em>*) with operations common in aggregators.
 *
 * <p>The class attempts to closely mirror the semantics of {@link java.nio.ByteBuffer}.
 * Unfortunately, it is not possible for us to implement the actual interface since {@link
 * java.nio.Buffer} and {@link java.nio.ByteBuffer} have non-visible constructors.
 *
 * <p>Like {@code ByteBuffer}, this class provides both relative and absolute <em>get</em> and
 * <em>put</em> methods. Unlike {@code ByteBuffer}, however, we distinguish the relative methods by
 * calling them <em>getNext</em> and <em>putNext</em>
 */
public class ByteSlice {

  /**
   * Creates a new byte slice. The slice's position will be zero, its limit will be its capacity and
   * each of its elements will be initialized to zero. The backing array will be mutable and its
   * array offset will be zero.
   */
  public static ByteSlice allocate(int capacity) {
    Preconditions.checkArgument(capacity >= 0);
    ByteSlice slice = new ByteSlice();
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
   * <p>This is equivalent to <code>{@link #copyOnWrite(byte[], int, int) copyOnWrite}(data, 0,
   * data.length)</code>.
   */
  public static ByteSlice copyOnWrite(byte[] array) {
    ByteSlice slice = new ByteSlice();
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
   * <p>This is equivalent to <code>{@link #copyOnWrite(byte[], int, int) copyOnWrite}(data, 0,
   * data.length)</code>.
   */
  public static ByteSlice copyOnWrite(byte[] array, int offset, int length) {
    int limit = offset + length;
    Preconditions.checkPositionIndexes(offset, limit, array.length);
    ByteSlice slice = new ByteSlice();
    slice.initForCopyOnWrite(array, 0, offset, limit);
    return slice;
  }

  /**
   * Wraps the backing array of a {@link ByteBuffer} into a new byte slice. The backing array, the
   * array offset, the position and the limit will be taken from the buffer.
   *
   * @throws UnsupportedOperationException if the buffer is not backed by an accessible array.
   */
  public static ByteSlice copyOnWrite(ByteBuffer buffer) {
    ByteSlice slice = new ByteSlice();
    slice.initForCopyOnWrite(
        buffer.array(), buffer.arrayOffset(), buffer.position(), buffer.limit());
    return slice;
  }

  /**
   * Wraps the backing array of a {@link ByteSlice} into a new byte slice. The backing array, the
   * array offset, the position and the limit will be taken from the other slice.
   */
  public static ByteSlice copyOnWrite(ByteSlice source) {
    ByteSlice slice = new ByteSlice();
    slice.initForCopyOnWrite(
        source.array(), source.arrayOffset(), source.position(), source.limit());
    return slice;
  }

  /**
   * Used to capture results from getVarInt, which expects an int[] as an argument for the decoded
   * varint. We make this an instance variable rather than a local variable in the method to avoid
   * the array allocation cost. This class isn't designed to be threadsafe anyway.
   */
  private final int[] result = new int[1];

  /** The byte array that backs this buffer. */
  protected byte[] array;

  /** The offset within this slice's backing array of the first element of the slice. */
  protected int arrayOffset;

  /** Whether the array must be copied before it can be modified. */
  protected boolean copyOnWrite;

  /** Index of the first element that should not be read or written. */
  protected int limit;

  /** Index of the next element to be read or written. */
  protected int position;

  // Clients should use factory methods.
  ByteSlice() {}

  /**
   * Returns the array that backs this buffer.
   *
   * <p>Callers should be careful not to modify this array if {@link #isCopyOnWrite()} is {@code
   * true}. Instead, callers should first call {@link #ensureWriteable()} which will create a copy
   * of the array.
   */
  public byte[] array() {
    return array;
  }

  /** Returns the offset within this slice's backing array of the first element of the slice. */
  public int arrayOffset() {
    return arrayOffset;
  }

  /**
   * Returns a ByteBuffer representation of this slice. The buffer's position and limit will
   * correspond to the slice's position and limit. If the slice is marked as {@link #copyOnWrite}
   * the buffer will be read-only.
   */
  public ByteBuffer byteBuffer() {
    // Create a ByteBuffer that starts at the array offset.
    ByteBuffer buffer = ByteBuffer.wrap(array);
    buffer.position(arrayOffset);
    buffer = buffer.slice();

    // Update the position and limit to match.
    buffer.position(position);
    buffer.limit(limit);

    if (isCopyOnWrite()) {
      return buffer.asReadOnlyBuffer();
    }
    return buffer;
  }

  /** Returns this slice's total capacity. */
  public int capacity() {
    int capacity = array.length - arrayOffset;
    assert 0 <= capacity;
    assert position <= capacity;
    assert limit <= capacity;
    return capacity;
  }

  /** Clears this slice. The position is set to zero and the limit is set to the capacity. */
  public ByteSlice clear() {
    limit = array.length - arrayOffset;
    position = 0;
    return this;
  }

  /**
   * Copies the backing byte array if the slice is {@link #isCopyOnWrite()}. Otherwise, does
   * nothing. The backing byte array will be copied from the array offset up to its length.
   * Afterwards, the array offset will be 0, {@link #isCopyOnWrite()} will be false and the position
   * and limit will be unchanged.
   */
  public ByteSlice ensureWriteable() {
    if (copyOnWrite) {
      array = Arrays.copyOfRange(array, arrayOffset, array.length);
      arrayOffset = 0;
      copyOnWrite = false;
    }
    return this;
  }

  /**
   * Flips this slice. The limit is set to the current position and then the position is set to
   * zero. This is useful to prepare a slice for reading after a sequence of <em>put</em> calls.
   */
  public ByteSlice flip() {
    limit = position;
    position = 0;
    return this;
  }

  /**
   * Relative <em>get</em> method for reading a variable-length encoded integer. Increments the
   * position by the number of bytes read.
   *
   * @throws IndexOutOfBoundsException if there are no more bytes to read, if the varint is too long
   *     or if it is truncated
   */
  public int getNextVarInt() {
    position = getVarInt(position, result);
    return result[0];
  }

  /**
   * Absolute <em>get</em> method for reading a variable-length encoded integer.
   *
   * @throws IndexOutOfBoundsException if there are no more bytes to read, if the varint is too long
   *     or if it is truncated
   */
  public int getVarInt(int index) {
    int[] result = new int[1];
    getVarInt(index, result);
    return result[0];
  }

  /** Returns whether the position is less than the limit. */
  public boolean hasRemaining() {
    return position < limit;
  }

  /** Returns whether the backing array will be copied before any write takes place. */
  public boolean isCopyOnWrite() {
    return copyOnWrite;
  }

  /** Returns this slice's limit. */
  public int limit() {
    assert 0 <= limit;
    assert position <= limit;
    assert limit <= array.length - arrayOffset;
    return limit;
  }

  /**
   * Sets this slice's limit. If the position is larger than the new limit then it is set to the new
   * limit.
   *
   * @param newLimit the new limit value; must be non-negative and no larger than this slice's
   *     capacity
   */
  public ByteSlice limit(int newLimit) {
    if (newLimit < 0 || newLimit > capacity()) {
      throw new IndexOutOfBoundsException(
          "limit must be between 0 and " + capacity() + " (capacity) but was " + newLimit);
    }
    limit = newLimit;
    if (position > newLimit) {
      position = newLimit;
    }
    return this;
  }

  /** Returns this slice's position. */
  public int position() {
    assert 0 <= position;
    assert position <= limit;
    assert position <= array.length - arrayOffset;
    return position;
  }

  /**
   * Sets this buffer's position.
   *
   * @param newPosition the new position value; must be non-negative and no larger than the current
   *     limit
   */
  public ByteSlice position(int newPosition) {
    if (newPosition < 0 || newPosition > limit) {
      throw new IndexOutOfBoundsException(
          "position must be between 0 and " + limit + " (current limit) but was " + newPosition);
    }
    position = newPosition;
    return this;
  }

  /**
   * Absolute <em>put</em> method to set a value to the maximum of {@code b} or the current value.
   *
   * @throws IndexOutOfBoundsException if {@code index} is negative or not smaller than the slice's
   *     limit
   */
  public ByteSlice putMax(int index, byte b) {
    checkPositionIndex(index, limit - 1);

    // Code is simpler if we can assume arrayOffset = 0 after a copy-on-write.
    ensureWriteable();
    assert !copyOnWrite;
    assert arrayOffset == 0;

    array[index] = array[index] < b ? b : array[index];

    return this;
  }

  /**
   * Absolute bulk <em>put</em> method to set the values starting at {@code index} to the maximum of
   * their current value of the corresponding value in {@code src}.
   *
   * @throws IndexOutOfBoundsException if {@code index} is negative or if the limit is smaller than
   *     {@code index + src.remaining()}.
   */
  public ByteSlice putMax(int index, ByteSlice src) {
    checkPositionIndex(index, limit);
    checkPositionIndex(index + src.remaining(), limit);

    // Code is simpler if we can assume arrayOffset = 0 after a copy-on-write.
    ensureWriteable();
    assert !copyOnWrite;
    assert arrayOffset == 0;

    int remaining = src.remaining();
    int srcOffset = src.arrayOffset + src.position;
    for (int i = 0; i < remaining; i++) {
      byte b = src.array[srcOffset + i];
      array[index + i] = array[index + i] < b ? b : array[index + i];
    }

    return this;
  }

  /**
   * Relative <em>put</em> method for writing a variable-encoded integer. Writes the integer value
   * starting at the current slice position and increments the position with the number of bytes
   * written.
   *
   * @throws IndexOutOfBoundsException if writing the integer would cause the position to move past
   *     the limit.
   */
  public ByteSlice putNextVarInt(int value) {
    checkPositionIndex(position, limit);
    checkPositionIndex(position + VarInt.varIntSize(value), limit);
    ensureWriteable();
    position = uncheckedPutVarInt(position, value);
    return this;
  }

  /**
   * Absolute <em>put</em> method for writing a variable-encoded integer.
   *
   * @throws IndexOutOfBoundsException if the index is negative or if writing the integer would
   *     cause the position to move past the limit.
   */
  public ByteSlice putVarInt(int index, int value) {
    checkPositionIndex(index, limit);
    checkPositionIndex(index + VarInt.varIntSize(value), limit);
    ensureWriteable();
    uncheckedPutVarInt(index, value);
    return this;
  }

  /** Returns the number of bytes between the current position and the limit. */
  public int remaining() {
    assert position <= limit;
    return limit - position;
  }

  /**
   * Returns a copy of the remaining bytes between the position and the limit. Does not modify the
   * position or the limit.
   */
  public byte[] toByteArray() {
    return Arrays.copyOfRange(array, arrayOffset + position, arrayOffset + limit);
  }

  /**
   * Returns a copy of the remaining bytes between the position and the limit as an unmodifiable
   * ByteString. Does not modify the position or the limit.
   */
  public ByteString toByteString() {
    // Don't use UnsafeByteOperations here since we can't guarantee that a client won't modify the
    // buffer afterwards. Clients who know that they won't modify the buffer can still use
    // UnsafeByteOperations themselves by calling something like:
    //   UnsafeByteOperations.unsafeWrap(slice.array(), slice.position(), slice.remaining());
    return ByteString.copyFrom(array, arrayOffset + position, limit - position);
  }

  private int getVarInt(int index, int[] result) {
    int newPos = VarInt.getVarInt(array, arrayOffset + index, result);
    if (newPos > arrayOffset + limit) {
      throw new IndexOutOfBoundsException();
    }
    return newPos - arrayOffset;
  }

  /** Initializes the state by allocating a new backing byte array of the given capacity. */
  protected void initForAllocate(int capacity) {
    array = new byte[capacity];
    arrayOffset = 0;
    copyOnWrite = false;
    position = 0;
    limit = capacity;
  }

  /** Initializes the state creating a copy-on-write view of (a subset of) an array. */
  protected void initForCopyOnWrite(byte[] array, int arrayOffset, int position, int limit) {
    this.array = array;
    this.arrayOffset = arrayOffset;
    this.copyOnWrite = true;
    this.position = position;
    this.limit = limit;
  }

  /**
   * Writes a varint value at the given index. Assumes that we are allowed to write into the backing
   * array, that its array offset is 0 and that it has sufficient capacity.
   */
  protected final int uncheckedPutVarInt(int index, int value) {
    // Check preconditions during debug / testing.
    assert !copyOnWrite;
    assert arrayOffset == 0;
    assert array.length >= index + VarInt.varIntSize(value);

    return VarInt.putVarInt(value, array, index);
  }
}
