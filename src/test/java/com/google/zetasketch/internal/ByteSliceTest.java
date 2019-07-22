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
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Tests for {@link ByteSlice}. */
@RunWith(JUnit4.class)
public class ByteSliceTest {

  private static byte[] varInt(int v) {
    try {
      ByteArrayOutputStream stream = new ByteArrayOutputStream();
      VarInt.putVarInt(v, stream);
      return stream.toByteArray();
    } catch (IOException e) {
      throw new AssertionError(e);
    }
  }

  @Test
  public void allocate() {
    ByteSlice slice = ByteSlice.allocate(5);

    assertThat(slice.array()).isEqualTo(new byte[5]);
    assertFalse(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());
  }

  @Test
  public void allocate_Empty() {
    ByteSlice slice = ByteSlice.allocate(0);

    assertThat(slice.array()).isEmpty();
    assertFalse(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(0, slice.capacity());
    assertEquals(0, slice.position());
    assertEquals(0, slice.limit());
  }

  @Test
  public void allocate_Negative() {
    assertThrows(IllegalArgumentException.class, () -> ByteSlice.allocate(-1));
  }

  @Test
  public void byteBuffer() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});

    ByteBuffer byteBuffer = slice.byteBuffer();
    assertEquals(0, byteBuffer.position());
    assertEquals(5, byteBuffer.limit());
    assertThat(byteBufferToArray(byteBuffer)).isEqualTo(new byte[] {1, 2, 3, 4, 5});

    slice.position(1);
    byteBuffer = slice.byteBuffer();
    assertEquals("Buffer should not be sliced", 1, byteBuffer.position());
    assertThat(byteBufferToArray(byteBuffer)).isEqualTo(new byte[] {2, 3, 4, 5});

    slice.limit(4);
    byteBuffer = slice.byteBuffer();
    assertThat(byteBufferToArray(byteBuffer)).isEqualTo(new byte[] {2, 3, 4});
  }

  @Test
  public void byteBuffer_ReadOnly() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertTrue(slice.byteBuffer().isReadOnly());

    slice.ensureWriteable();
    assertFalse(slice.byteBuffer().isReadOnly());
  }

  @Test
  public void byteBuffer_WithArrayOffset() {
    ByteSlice slice = ByteSlice.copyOnWrite(
        ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}, 1, 3).slice());

    ByteBuffer byteBuffer = slice.byteBuffer();
    assertEquals("Buffer should be sliced", 0, byteBuffer.position());
    assertEquals(3, byteBuffer.limit());
    assertThat(byteBufferToArray(byteBuffer)).isEqualTo(new byte[] {2, 3, 4});

    slice.position(1);
    assertThat(byteBufferToArray(slice.byteBuffer())).isEqualTo(new byte[] {3, 4});

    slice.limit(2);
    assertThat(byteBufferToArray(slice.byteBuffer())).isEqualTo(new byte[] {3});
  }

  @Test
  public void clear() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteSlice slice = ByteSlice.copyOnWrite(array, 1, 3);

    slice.clear();
    assertSame(array, slice.array());
    assertEquals(0, slice.arrayOffset());
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());
  }

  @Test
  public void clear_WithArrayOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    slice.clear();
    assertSame(array, slice.array());
    assertEquals(1, slice.arrayOffset());
    assertEquals(0, slice.position());
    assertEquals(4, slice.limit());
  }

  @Test
  public void copyOnWrite_FromArray() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());
  }

  @Test
  public void copyOnWrite_FromArrayWithBounds() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteSlice slice = ByteSlice.copyOnWrite(array, 1, 3);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(1, slice.position());
    assertEquals(4, slice.limit());
  }

  @Test
  public void copyOnWrite_FromArrayWithNegativeLength() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSlice.copyOnWrite(array, 2, -1));
  }

  @Test
  public void copyOnWrite_FromArrayWithNegativeOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSlice.copyOnWrite(array, -1, 3));
  }

  @Test
  public void copyOnWrite_FromArrayWithOutOfBoundsLength() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSlice.copyOnWrite(array, 4, 2));
  }

  @Test
  public void copyOnWrite_FromArrayWithOutOfBoundsOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    assertThrows(IndexOutOfBoundsException.class, () -> ByteSlice.copyOnWrite(array, 5, 1));
  }

  @Test
  public void copyOnWrite_FromArrayWithZeroLength() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteSlice slice = ByteSlice.copyOnWrite(array, 1, 0);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(1, slice.position());
    assertEquals(1, slice.limit());
  }

  @Test
  public void copyOnWrite_FromByteBuffer() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array);
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());
  }

  @Test
  public void copyOnWrite_FromByteSlice() {
    ByteSlice source = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5}, 1, 3);
    source.ensureWriteable();

    ByteSlice slice = ByteSlice.copyOnWrite(source);
    assertSame(source.array(), slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(1, slice.position());
    assertEquals(4, slice.limit());
  }

  @Test
  public void copyOnWrite_FromByteBufferWithArrayOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(1, slice.arrayOffset());
    assertEquals(4, slice.capacity());
    assertEquals(0, slice.position());
    assertEquals(3, slice.limit());
  }

  @Test
  public void copyOnWrite_FromByteBufferWithBounds() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array);
    buffer.position(1);
    buffer.limit(4);
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(5, slice.capacity());
    assertEquals(1, slice.position());
    assertEquals(4, slice.limit());
  }

  @Test
  public void copyOnWrite_FromByteBufferWithoutArray() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[3]).asReadOnlyBuffer();
    assertThrows(UnsupportedOperationException.class, () -> ByteSlice.copyOnWrite(buffer));
  }

  @Test
  public void copyOnWrite_FromEmptyArray() {
    byte[] array = new byte[0];
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    assertSame(array, slice.array());
    assertTrue(slice.isCopyOnWrite());
    assertEquals(0, slice.arrayOffset());
    assertEquals(0, slice.capacity());
    assertEquals(0, slice.position());
    assertEquals(0, slice.limit());
  }

  @Test
  public void ensureWriteable_WhenNotCopyOnWrite() {
    ByteSlice slice = ByteSlice.allocate(5);
    byte[] array = slice.array();

    slice.ensureWriteable();
    assertSame(array, slice.array());
    assertFalse(slice.isCopyOnWrite());
  }

  @Test
  public void ensureWriteable_WithArrayOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    slice.ensureWriteable();
    assertNotSame(array, slice.array());
    assertThat(slice.array()).isEqualTo(new byte[] {2, 3, 4, 5});
    assertEquals(0, slice.arrayOffset());
    assertEquals(0, slice.position());
    assertEquals(3, slice.limit());
    assertFalse(slice.isCopyOnWrite());
  }

  @Test
  public void flip() {
    ByteSlice slice = ByteSlice.allocate(5);
    slice.flip();
    assertEquals(0, slice.position());
    assertEquals(0, slice.limit());

    slice.limit(5);
    slice.position(3);
    slice.flip();
    assertEquals(0, slice.position());
    assertEquals(3, slice.limit());
  }

  @Test
  public void getVarInt() throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(new byte[2]);
    VarInt.putVarInt(45678, stream);
    ByteSlice slice = ByteSlice.copyOnWrite(stream.toByteArray());

    assertEquals(45678, slice.getVarInt(2));
    assertEquals(0, slice.position());
  }

  /** Absolute varint read should throw an exception when the index is negative. */
  @Test
  public void getVarInt_Negative() throws Exception {
    ByteSlice slice = ByteSlice.copyOnWrite(varInt(1234567890));

    assertThrows(IndexOutOfBoundsException.class, () -> slice.getVarInt(-1));
  }

  /** Absolute varint read should throw an exception when the index is at or past the limit. */
  @Test
  public void getVarInt_OutOfBounds() throws Exception {
    ByteSlice slice = ByteSlice.copyOnWrite(varInt(1234567890));
    slice.limit(0);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.getVarInt(0));
  }

  /** Absolute read of a truncated varint should throw an exception. */
  @Test
  public void getVarInt_Truncated() throws Exception {
    byte[] array = varInt(1234567890);
    ByteSlice slice = ByteSlice.copyOnWrite(array, 0, array.length - 1);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.getVarInt(0));
  }

  @Test
  public void getNextVarInt() throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    VarInt.putVarInt(123, stream);
    VarInt.putVarInt(456, stream);
    VarInt.putVarInt(7890, stream);
    ByteSlice slice = ByteSlice.copyOnWrite(stream.toByteArray());

    assertEquals(123, slice.getNextVarInt());
    assertEquals(VarInt.varIntSize(123), slice.position());

    assertEquals(456, slice.getNextVarInt());
    assertEquals(VarInt.varIntSize(123) + VarInt.varIntSize(456), slice.position());

    assertEquals(7890, slice.getNextVarInt());
    assertEquals(slice.limit(), slice.position());
  }

  /** Relative varint read should throw an exception when the position is at the limit. */
  @Test
  public void getNextVarInt_OutOfBounds() throws Exception {
    ByteSlice slice = ByteSlice.allocate(5);
    slice.position(5);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.getNextVarInt());

    assertEquals("Position should not have changed", 5, slice.position());
  }

  /**
   * Relative read of a truncated varint should throw an exception and should not update the
   * position.
   */
  @Test
  public void getNextVarInt_Truncated() throws Exception {
    byte[] array = varInt(1234567890);
    ByteSlice slice = ByteSlice.copyOnWrite(array, 0, array.length - 1);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.getNextVarInt());

    assertEquals("Position should not have changed", 0, slice.position());
  }

  @Test
  public void getNextVarInt_WithArrayOffset() throws Exception {
    ByteArrayOutputStream stream = new ByteArrayOutputStream();
    stream.write(new byte[2]);
    VarInt.putVarInt(123, stream);
    VarInt.putVarInt(7890, stream);
    byte[] array = stream.toByteArray();

    ByteSlice slice = ByteSlice.copyOnWrite(ByteBuffer.wrap(array, 2, array.length - 2).slice());
    assertEquals(2, slice.arrayOffset());

    assertEquals(123, slice.getNextVarInt());
    assertEquals(VarInt.varIntSize(123), slice.position());

    assertEquals(7890, slice.getNextVarInt());
    assertEquals(slice.limit(), slice.position());
  }

  @Test
  public void hasRemaining() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertTrue(slice.hasRemaining());

    slice.position(3);
    assertTrue(slice.hasRemaining());

    slice.limit(4);
    assertTrue(slice.hasRemaining());

    slice.position(4);
    assertFalse(slice.hasRemaining());
  }

  @Test
  public void limit() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    slice.limit(4);
    assertEquals(4, slice.limit());
    assertEquals(0, slice.position());

    slice.position(4);
    slice.limit(3);
    assertEquals(3, slice.limit());
    assertEquals(3, slice.position());
  }

  @Test
  public void limit_Negative() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertThrows(IndexOutOfBoundsException.class, () -> slice.limit(-1));
  }

  @Test
  public void limit_OutOfBounds() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertThrows(IndexOutOfBoundsException.class, () -> slice.limit(6));
  }

  @Test
  public void limit_OutOfBoundsArrayOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.limit(5));
  }

  @Test
  public void limit_WithArrayOffset() {
    byte[] array = new byte[] {1, 2, 3, 4, 5};
    ByteBuffer buffer = ByteBuffer.wrap(array, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);

    slice.limit(4);
    assertEquals(4, slice.limit());
    assertEquals(0, slice.position());
  }

  @Test
  public void position() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    slice.position(4);
    assertEquals(4, slice.position());
    assertEquals(5, slice.limit());

    slice.position(5);
    assertEquals(5, slice.position());
    assertEquals(5, slice.limit());
  }

  @Test
  public void position_Negative() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});

    assertThrows(IndexOutOfBoundsException.class, () -> slice.position(-1));
  }

  @Test
  public void position_OutOfBounds() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    slice.limit(3);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.position(4));
  }

  @Test
  public void putMaxByte() {
    ByteSlice slice = ByteSlice.allocate(2);

    slice.putMax(0, (byte) 2);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {2, 0});

    slice.putMax(0, (byte) 0);
    slice.putMax(0, (byte) -1);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {2, 0});

    slice.putMax(0, (byte) 3);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3, 0});

    slice.putMax(1, (byte) 2);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3, 2});
  }

  /** Tests that a putMax calls ensureWriteable. */
  @Test
  public void putMaxByte_CopiesOnWrite() {
    byte[] array = new byte[] {1, 2};
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    slice.putMax(0, (byte) 4);
    assertThat(array).isEqualTo(new byte[] {1, 2});
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {4, 2});
  }

  @Test
  public void putMaxByte_IndexOutOfBounds() {
    ByteSlice slice = ByteSlice.allocate(2);
    slice.limit(1);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.putMax(1, (byte) 0));
  }

  @Test
  public void putMaxByte_NegativeIndex() {
    ByteSlice slice = ByteSlice.allocate(2);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.putMax(-1, (byte) 0));
  }

  @Test
  public void putMaxByteSlice() {
    ByteSlice slice = ByteSlice.allocate(4);

    slice.putMax(0, ByteSlice.copyOnWrite(new byte[] {1, 2, 3}));
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {1, 2, 3, 0});

    slice.putMax(1, ByteSlice.copyOnWrite(new byte[] {3, 2, 1}));
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {1, 3, 3, 1});
  }

  /** Tests that a putMax calls ensureWriteable. */
  @Test
  public void putMaxByteSlice_CopiesOnWrite() {
    byte[] array = new byte[] {1, 2};
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    slice.putMax(0, ByteSlice.copyOnWrite(new byte[] {4, 2}));
    assertThat(array).isEqualTo(new byte[] {1, 2});
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {4, 2});
  }

  @Test
  public void putMaxByteSlice_IndexOutOfBounds() {
    ByteSlice slice = ByteSlice.allocate(3);
    slice.limit(1);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> slice.putMax(1, ByteSlice.copyOnWrite(new byte[] {1, 2, 3})));
  }

  @Test
  public void putMaxByteSlice_NegativeIndex() {
    ByteSlice slice = ByteSlice.allocate(3);

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> slice.putMax(-1, ByteSlice.copyOnWrite(new byte[] {1, 2})));
  }

  @Test
  public void putMaxByteSlice_WithArrayOffset() {
    ByteSlice src = ByteSlice.copyOnWrite(ByteBuffer.wrap(new byte[] {1, 2, 3}, 1, 2).slice());
    ByteSlice dest = ByteSlice.allocate(3);

    dest.putMax(1, src);
    assertThat(dest.toByteArray()).isEqualTo(new byte[] {0, 2, 3});
  }

  @Test
  public void putVarInt() {
    ByteSlice slice = ByteSlice.allocate(10);

    byte[] expected = new byte[10];
    VarInt.putVarInt(1234, expected, 1);

    slice.putVarInt(1, 1234);
    assertThat(slice.array()).isEqualTo(expected);
  }

  /** Tests that a putVarInt calls ensureWriteable. */
  @Test
  public void putVarInt_CopiesOnWrite() {
    byte[] array = new byte[5];
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    byte[] expected = new byte[5];
    VarInt.putVarInt(1234, expected, 1);

    slice.putVarInt(1, 1234);
    assertThat(array).isEqualTo(new byte[5]);
    assertThat(slice.array()).isEqualTo(expected);
  }

  @Test
  public void putVarInt_Overflow() {
    ByteSlice slice = ByteSlice.allocate(5);
    slice.limit(4);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.putVarInt(3, 1234));

    // Check that no bytes were written.
    assertThat(slice.array()).isEqualTo(new byte[5]);
  }

  @Test
  public void putNextVarInt() {
    ByteSlice slice = ByteSlice.allocate(10);
    slice.position(1);

    byte[] expected = new byte[10];
    VarInt.putVarInt(1234, expected, 1);

    slice.putNextVarInt(1234);
    assertEquals(slice.position(), 1 + VarInt.varIntSize(1234));
    assertThat(slice.array()).isEqualTo(expected);
  }

  /** Tests that a putNextVarInt calls ensureWriteable. */
  @Test
  public void putNextVarInt_CopiesOnWrite() {
    byte[] array = new byte[5];
    ByteSlice slice = ByteSlice.copyOnWrite(array);

    slice.putNextVarInt(1234);
    assertThat(array).isEqualTo(new byte[5]);
    assertThat(slice.flip().toByteArray()).isEqualTo(varInt(1234));
  }

  @Test
  public void putNextVarInt_Overflow() {
    ByteSlice slice = ByteSlice.allocate(6);
    slice.position(1);
    slice.limit(2);

    assertThrows(IndexOutOfBoundsException.class, () -> slice.putNextVarInt(1234));

    // Check that no bytes were written.
    assertThat(slice.array()).isEqualTo(new byte[6]);
  }

  @Test
  public void remaining() {
    ByteSlice slice = ByteSlice.allocate(5);
    assertEquals(5, slice.remaining());

    slice.position(1);
    assertEquals(4, slice.remaining());

    slice.limit(3);
    assertEquals(2, slice.remaining());

    slice.position(3);
    assertEquals(0, slice.remaining());
  }

  @Test
  public void toByteArray() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {1, 2, 3, 4, 5});
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());

    slice.position(2);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3, 4, 5});

    slice.limit(4);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3, 4});
  }

  @Test
  public void toByteArray_WithArrayOffset() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {2, 3, 4});

    slice.position(1);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3, 4});

    slice.limit(2);
    assertThat(slice.toByteArray()).isEqualTo(new byte[] {3});
  }

  @Test
  public void toByteString() {
    ByteSlice slice = ByteSlice.copyOnWrite(new byte[] {1, 2, 3, 4, 5});
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {1, 2, 3, 4, 5});
    assertEquals(0, slice.position());
    assertEquals(5, slice.limit());

    slice.position(2);
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {3, 4, 5});

    slice.limit(4);
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {3, 4});
  }

  @Test
  public void toByteString_WithArrayOffset() {
    ByteBuffer buffer = ByteBuffer.wrap(new byte[] {1, 2, 3, 4, 5}, 1, 3).slice();
    ByteSlice slice = ByteSlice.copyOnWrite(buffer);
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {2, 3, 4});

    slice.position(1);
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {3, 4});

    slice.limit(2);
    assertThat(slice.toByteString().toByteArray()).isEqualTo(new byte[] {3});
  }

  private byte[] byteBufferToArray(ByteBuffer buffer) {
    byte[] array = new byte[buffer.remaining()];
    buffer.get(array);
    return array;
  }
}
