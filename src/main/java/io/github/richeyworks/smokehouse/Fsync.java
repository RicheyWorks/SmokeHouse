package io.github.richeyworks.smokehouse;

/**
 * Durability dial for the segment log (ADR D3: the log IS the WAL — this is the only fsync that
 * exists). Phase 1 ships the two ends of the dial; the grouped {@code INTERVAL(ms)} middle —
 * the planned default — is Phase 2.
 */
public enum Fsync {

    /** {@code force} after every append: nothing acknowledged is ever lost; slowest. */
    ALWAYS,

    /**
     * Let the OS page cache decide: fastest; a crash loses recently-acknowledged appends
     * (never corrupts — a torn tail is truncated by CRC at recovery).
     */
    OS
}
