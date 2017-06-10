/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.transforms.splittabledofn;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import org.apache.beam.sdk.transforms.DoFn;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A {@link RestrictionTracker} for claiming offsets in an {@link OffsetRange} in a monotonically
 * increasing fashion.
 */
public class OffsetRangeTracker implements RestrictionTracker<OffsetRange> {
  private static final Logger LOG = LoggerFactory.getLogger(OffsetRangeTracker.class);

  private OffsetRange range;
  private Long lastClaimedOffset = null;
  private Long lastAttemptedOffset = null;

  public OffsetRangeTracker(OffsetRange range) {
    this.range = checkNotNull(range);
  }

  @Override
  public synchronized OffsetRange currentRestriction() {
    return range;
  }

  @Override
  public synchronized OffsetRange checkpoint() {
    if (lastClaimedOffset == null) {
      OffsetRange res = range;
      range = new OffsetRange(range.getFrom(), range.getFrom());
      return res;
    }
    OffsetRange res = new OffsetRange(lastClaimedOffset + 1, range.getTo());
    this.range = new OffsetRange(range.getFrom(), lastClaimedOffset + 1);
    return res;
  }

  /**
   * Attempts to claim the given offset.
   *
   * <p>Must be larger than the last successfully claimed offset.
   *
   * @return {@code true} if the offset was successfully claimed, {@code false} if it is outside the
   *     current {@link OffsetRange} of this tracker (in that case this operation is a no-op).
   */
  public synchronized boolean tryClaim(long i) {
    checkArgument(
        lastAttemptedOffset == null || i > lastAttemptedOffset,
        "Trying to claim offset %s while last attempted was %s",
        i,
        lastAttemptedOffset);
    checkArgument(
        i >= range.getFrom(), "Trying to claim offset %s before start of the range %s", i, range);
    lastAttemptedOffset = i;
    // No respective checkArgument for i < range.to() - it's ok to try claiming offsets beyond it.
    if (i >= range.getTo()) {
      return false;
    }
    lastClaimedOffset = i;
    return true;
  }

  @Override
  public synchronized double getFractionClaimed() {
    if (lastAttemptedOffset == null) {
      return 0.0;
    }
    // E.g., when reading [3, 6) and lastRecordStart is 4, that means we consumed 3 of 3,4,5
    // which is (4 - 3) / (6 - 3) = 33%.
    // Also, clamp to at most 1.0 because the last consumed position can extend past the
    // stop position.
    return Math.min(
        1.0, 1.0 * (lastAttemptedOffset - range.getFrom()) / (range.getTo() - range.getFrom()));
  }

  @Override
  public String toString() {
    return "OffsetRangeTracker{" +
        "range=" + range +
        ", lastClaimedOffset=" + lastClaimedOffset +
        ", lastAttemptedOffset=" + lastAttemptedOffset +
        '}';
  }

  @Override
  public synchronized OffsetRange splitRemainderAfterFraction(double fractionOfRemainder) {
    LOG.info("ORT {} - requesting splitRemainderAfterFraction {}", this, fractionOfRemainder);
    long remainderStart = (lastAttemptedOffset == null) ? range.getFrom() : lastAttemptedOffset;
    long splitOffset =
        remainderStart + (long) Math.floor(fractionOfRemainder * (range.getTo() - remainderStart));
    LOG.info("Split offset is {}", splitOffset);
    if (lastClaimedOffset != null && splitOffset <= lastClaimedOffset) {
      LOG.info("Rejecting split request - before last claimed");
      return null;
    }
    if (splitOffset >= range.getTo()) {
      LOG.info("Rejecting split request - past end of range");
      return null;
    }
    OffsetRange primary = new OffsetRange(range.getFrom(), splitOffset);
    OffsetRange residual = new OffsetRange(splitOffset, range.getTo());
    this.range = primary;
    LOG.info("Accepted split request - now {}", this);
    return residual;
  }

  /**
   * Marks that there are no more offsets to be claimed in the range.
   *
   * <p>E.g., a {@link DoFn} reading a file and claiming the offset of each record in the file might
   * call this if it hits EOF - even though the last attempted claim was before the end of the
   * range, there are no more offsets to claim.
   */
  public synchronized void markDone() {
    lastAttemptedOffset = Long.MAX_VALUE;
  }

  @Override
  public synchronized void checkDone() throws IllegalStateException {
    checkState(
        lastAttemptedOffset >= range.getTo() - 1,
        "Last attempted offset was %s in range %s, claiming work in [%s, %s) was not attempted",
        lastAttemptedOffset,
        range,
        lastAttemptedOffset + 1,
        range.getTo());
  }
}
