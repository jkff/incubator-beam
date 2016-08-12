package org.apache.beam.sdk.transforms.splittabledofn;

import org.apache.beam.sdk.transforms.DoFn;

/**
 * Manages concurrent access to the restriction and keeps track of its claimed part for a <a
 * href="https://s.apache.org/splittable-do-fn>splittable</a> {@link DoFn}.
 */
public interface RestrictionTracker<RestrictionT> {
  /**
   * Returns a restriction accurately describing the full range of work the current {@link
   * DoFn.ProcessElement} call will do, including already completed work.
   */
  RestrictionT currentRestriction();

  /**
   * Signals that the current {@link DoFn.ProcessElement} call should terminate as soon as possible.
   * Modifies {@link #currentRestriction}. Returns a restriction representing the rest of the work:
   * the old value of {@link #currentRestriction} is equivalent to the new value and the return
   * value of this method combined.
   */
  RestrictionT checkpoint();

  // TODO: Add the more general splitRemainderAfterFraction() and other methods.
}
