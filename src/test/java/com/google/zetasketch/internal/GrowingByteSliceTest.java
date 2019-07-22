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
import static org.junit.Assert.assertThrows;

import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link GrowingByteSlice}. */
@RunWith(JUnit4.class)
@SuppressWarnings("boxing")
public class GrowingByteSliceTest {

  /**
   * Returns the names of accessible instance methods of {@link GrowingByteSlice} that match the
   * predicate.
   */
  private static Set<String> instanceMethods(Predicate<Method> predicate) {
    return Arrays.stream(GrowingByteSlice.class.getMethods())
        .filter(m -> !m.isSynthetic())
        .filter(m -> !Modifier.isPrivate(m.getModifiers()))
        .filter(m -> !Modifier.isStatic(m.getModifiers()))
        .filter(predicate)
        .map(m -> m.getName())
        .collect(Collectors.toSet());
  }

  /**
   * Tests that the capacity is not increased on {@code ensureWriteable}, regardless of how many
   * bytes there are remaining. Relative {@code putNext} put calls will increase the capacity and so
   * calls to {@code ensureWriteable} can assume that writes are absolute or mostly within the
   * capacity bounds. At the same time, the capacity <em>must not</em> be decreased and all bytes
   * must be copied since the user may increase the limit after an {@code ensureWriteable} call to
   * read from those bytes.
   */
  @Test
  public void ensureWriteable_KeepsCapacity() {
    byte[] array = new byte[20];
    Arrays.fill(array, (byte) 1);
    ByteSlice slice;

    // Lots of remaining bytes.
    slice = GrowingByteSlice.copyOnWrite(array);
    slice.ensureWriteable();
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.array()).isEqualTo(array);

    // Few remaining bytes.
    slice = GrowingByteSlice.copyOnWrite(array, 20, 0);
    slice.ensureWriteable();
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.array()).isEqualTo(array);
  }

  @Test
  public void limit() {
    byte[] array = new byte[5];
    ByteSlice slice = GrowingByteSlice.copyOnWrite(array);

    // Should not copy if limit within capacity.
    slice.limit(3);
    assertThat(slice.array()).isSameInstanceAs(array);

    slice.limit(5);
    assertThat(slice.array()).isSameInstanceAs(array);

    // Copy once limit is past capacity as reads would fail otherwise.
    slice.limit(6);
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
  }

  @Test
  public void limit_Negative() {
    ByteSlice slice = GrowingByteSlice.copyOnWrite(new byte[] {1, 2});

    assertThrows(
        IndexOutOfBoundsException.class,
        () -> slice.limit(-1));
  }

  @Test
  public void limit_ReducesPosition() {
    ByteSlice slice = GrowingByteSlice.allocate(5);
    slice.position(3);

    slice.limit(4);
    assertEquals(3, slice.position());

    slice.limit(2);
    assertEquals(2, slice.position());
  }

  /** Tests that no methods return {@link ByteSlice} which is important for proper chaining. */
  @Test
  public void narrowsReturnType() {
    assertThat(instanceMethods(m -> m.getReturnType() == ByteSlice.class)).isEmpty();
  }

  /**
   * Tests that all {@link ByteSlice} methods starting with {@code put} are overridden by {@link
   * GrowingByteSliceTest} so that it can grow the backing array if needed.
   */
  @Test
  public void overridesPutMethods() {
    assertThat(
            instanceMethods(
                m -> m.getName().startsWith("put") && m.getDeclaringClass() == ByteSlice.class))
        .isEmpty();
  }

  @Test
  public void position() {
    byte[] array = new byte[5];
    ByteSlice slice = GrowingByteSlice.copyOnWrite(array);
    slice.limit(4);

    // Should not copy if limit within capacity.
    slice.position(3);
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(4, slice.limit());

    // Limit is increased, but still within capacity.
    slice.position(5);
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(5, slice.limit());

    // Copy once limit is past capacity.
    slice.position(6);
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
    assertEquals(6, slice.limit());
  }

  @Test
  public void putMaxByte() {
    ByteSlice slice = GrowingByteSlice.allocate(5);
    byte[] array = slice.array();
    slice.limit(2);

    slice.putMax(0, (byte) 1);
    assertThat(slice.array()).isSameInstanceAs(array);

    // Put past limit, but still has capacity.
    slice.putMax(2, (byte) 1);
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(3, slice.limit());

    // Put past capacity.
    slice.putMax(5, (byte) 1);
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
    assertEquals(6, slice.limit());
  }

  @Test
  public void putMaxByteSlice() {
    ByteSlice slice = GrowingByteSlice.allocate(5);
    byte[] array = slice.array();
    slice.limit(3);

    slice.putMax(0, GrowingByteSlice.allocate(2));
    assertThat(slice.array()).isSameInstanceAs(array);

    // Put past limit, but still has capacity.
    slice.putMax(2, GrowingByteSlice.allocate(2));
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(4, slice.limit());

    // Put past capacity.
    slice.putMax(4, GrowingByteSlice.allocate(2));
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
    assertEquals(6, slice.limit());
  }

  @Test
  public void putVarIntAbsolute() {
    ByteSlice slice = GrowingByteSlice.allocate(10);

    byte[] expected = new byte[10];
    VarInt.putVarInt(1234, expected, 1);

    slice.putVarInt(1, 1234);
    assertThat(slice.array()).isEqualTo(expected);
  }

  /** Tests that a putVarInt calls ensureWriteable. */
  @Test
  public void putVarIntAbsolute_CopiesOnWrite() {
    byte[] array = new byte[5];
    ByteSlice slice = GrowingByteSlice.copyOnWrite(array);

    slice.putVarInt(1, 1234);
    assertThat(array).isEqualTo(new byte[5]);
  }

  @Test
  public void putVarIntAbsolute_GrowsLimit() {
    ByteSlice slice = GrowingByteSlice.allocate(5);
    byte[] array = slice.array();
    slice.limit(3);

    slice.putVarInt(0, 1234);
    assertThat(slice.array()).isSameInstanceAs(array);

    // Put past limit, but still has capacity.
    slice.putVarInt(2, 4567);
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(4, slice.limit());

    // Put past capacity.
    slice.putVarInt(4, 8901);
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
    assertEquals(6, slice.limit());
  }

  @Test
  public void putNextVarInt() {
    ByteSlice slice = GrowingByteSlice.allocate(10);
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
    ByteSlice slice = GrowingByteSlice.copyOnWrite(array);

    slice.putNextVarInt(1234);
    assertThat(array).isEqualTo(new byte[5]);
  }

  @Test
  public void putNextVarInt_GrowsLimit() {
    ByteSlice slice = GrowingByteSlice.allocate(5);
    byte[] array = slice.array();
    slice.limit(3);

    slice.putNextVarInt(1234);
    assertThat(slice.array()).isSameInstanceAs(array);

    // Put past limit, but still has capacity.
    slice.putNextVarInt(4567);
    assertThat(slice.array()).isSameInstanceAs(array);
    assertEquals(4, slice.limit());

    // Put past capacity.
    slice.putNextVarInt(8901);
    assertThat(slice.array()).isNotSameInstanceAs(array);
    assertThat(slice.capacity()).isGreaterThan(6);
    assertEquals(6, slice.limit());
  }
}
