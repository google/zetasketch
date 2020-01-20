/*
 * Copyright (C) 2011 The Guava Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.zetasketch.internal.hash;

import static org.junit.Assert.assertEquals;

import com.google.common.hash.HashFunction;
import com.google.common.hash.PrimitiveSink;
import java.nio.ByteBuffer;
import java.util.Random;

/**
 * Partial copy of Guava's com.google.common.hash.HashTestUtils.
 *
 * <p>Various utilities for testing {@link HashFunction}s.
 *
 * @author Dimitris Andreou
 * @author Kurt Alfred Kluever
 */
final class HashTestUtils {
  private HashTestUtils() {}

  enum RandomHasherAction {
    PUT_BOOLEAN() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        boolean value = random.nextBoolean();
        for (PrimitiveSink sink : sinks) {
          sink.putBoolean(value);
        }
      }
    },
    PUT_BYTE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        int value = random.nextInt();
        for (PrimitiveSink sink : sinks) {
          sink.putByte((byte) value);
        }
      }
    },
    PUT_SHORT() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        short value = (short) random.nextInt();
        for (PrimitiveSink sink : sinks) {
          sink.putShort(value);
        }
      }
    },
    PUT_CHAR() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        char value = (char) random.nextInt();
        for (PrimitiveSink sink : sinks) {
          sink.putChar(value);
        }
      }
    },
    PUT_INT() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        int value = random.nextInt();
        for (PrimitiveSink sink : sinks) {
          sink.putInt(value);
        }
      }
    },
    PUT_LONG() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        long value = random.nextLong();
        for (PrimitiveSink sink : sinks) {
          sink.putLong(value);
        }
      }
    },
    PUT_FLOAT() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        float value = random.nextFloat();
        for (PrimitiveSink sink : sinks) {
          sink.putFloat(value);
        }
      }
    },
    PUT_DOUBLE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        double value = random.nextDouble();
        for (PrimitiveSink sink : sinks) {
          sink.putDouble(value);
        }
      }
    },
    PUT_BYTES() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        byte[] value = new byte[random.nextInt(128)];
        random.nextBytes(value);
        for (PrimitiveSink sink : sinks) {
          sink.putBytes(value);
        }
      }
    },
    PUT_BYTES_INT_INT() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        byte[] value = new byte[random.nextInt(128)];
        random.nextBytes(value);
        int off = random.nextInt(value.length + 1);
        int len = random.nextInt(value.length - off + 1);
        for (PrimitiveSink sink : sinks) {
          sink.putBytes(value, off, len);
        }
      }
    },
    PUT_BYTE_BUFFER() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        byte[] value = new byte[random.nextInt(128)];
        random.nextBytes(value);
        int pos = random.nextInt(value.length + 1);
        int limit = pos + random.nextInt(value.length - pos + 1);
        for (PrimitiveSink sink : sinks) {
          ByteBuffer buffer = ByteBuffer.wrap(value);
          buffer.position(pos);
          buffer.limit(limit);
          sink.putBytes(buffer);
          assertEquals(limit, buffer.limit());
          assertEquals(limit, buffer.position());
        }
      }
    },
    PUT_STRING() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        char[] value = new char[random.nextInt(128)];
        for (int i = 0; i < value.length; i++) {
          value[i] = (char) random.nextInt();
        }
        String s = new String(value);
        for (PrimitiveSink sink : sinks) {
          sink.putUnencodedChars(s);
        }
      }
    },
    PUT_STRING_LOW_SURROGATE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        String s = new String(new char[] {randomLowSurrogate(random)});
        for (PrimitiveSink sink : sinks) {
          sink.putUnencodedChars(s);
        }
      }
    },
    PUT_STRING_HIGH_SURROGATE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        String s = new String(new char[] {randomHighSurrogate(random)});
        for (PrimitiveSink sink : sinks) {
          sink.putUnencodedChars(s);
        }
      }
    },
    PUT_STRING_LOW_HIGH_SURROGATE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        String s = new String(new char[] {randomLowSurrogate(random), randomHighSurrogate(random)});
        for (PrimitiveSink sink : sinks) {
          sink.putUnencodedChars(s);
        }
      }
    },
    PUT_STRING_HIGH_LOW_SURROGATE() {
      @Override
      void performAction(Random random, Iterable<? extends PrimitiveSink> sinks) {
        String s = new String(new char[] {randomHighSurrogate(random), randomLowSurrogate(random)});
        for (PrimitiveSink sink : sinks) {
          sink.putUnencodedChars(s);
        }
      }
    };

    abstract void performAction(Random random, Iterable<? extends PrimitiveSink> sinks);

    private static final RandomHasherAction[] actions = values();

    static RandomHasherAction pickAtRandom(Random random) {
      return actions[random.nextInt(actions.length)];
    }
  }

  static char randomLowSurrogate(Random random) {
    return (char)
        (Character.MIN_LOW_SURROGATE
            + random.nextInt(Character.MAX_LOW_SURROGATE - Character.MIN_LOW_SURROGATE + 1));
  }

  static char randomHighSurrogate(Random random) {
    return (char)
        (Character.MIN_HIGH_SURROGATE
            + random.nextInt(Character.MAX_HIGH_SURROGATE - Character.MIN_HIGH_SURROGATE + 1));
  }
}
