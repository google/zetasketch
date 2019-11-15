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

package com.google.zetasketch.internal.hash;

import static org.junit.Assert.assertEquals;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** Unit tests for {@link Hash}. */
@RunWith(JUnit4.class)
public class HashTest {

  private static Hash hash = DefaultHash.HASH;

  @Test
  public void ofBytes() {

    assertEquals(0x23ad7c904aa665e3L, hash.of(new byte[] {}));
    assertEquals(0x36a1e57a138e4467L, hash.of(new byte[] {0x66, 0x6f, 0x6f, 0x62, 0x61, 0x72}));
  }

  @Test
  public void ofInt() {
    assertEquals(0x1f6e43ff4b5270eeL, hash.of(0));
    assertEquals(0x5cbded943bffddd3L, hash.of(42));
    assertEquals(0xfd5a96b7669422c1L, hash.of(-15));
  }

  @Test
  public void ofLong() {
    assertEquals(0x853a22bd6e14a48fL, hash.of(0L));
    assertEquals(0x583b2b9df8e0eb60L, hash.of(42L));
    assertEquals(0x539414f287f11e37L, hash.of(-15L));
  }

  @Test
  public void ofString() {
    assertEquals(0x23ad7c904aa665e3L, hash.of(""));
    assertEquals(0xd0bcbfe261b36504L, hash.of("foo"));
    assertEquals(0x27efc00f7d2ce548L, hash.of("Z\u00fcrich"));
    assertEquals(
        "Unicode strings must not be normalized", 0x7dfa3067e55c7e8aL, hash.of("Zu\u0308rich"));
  }
}
