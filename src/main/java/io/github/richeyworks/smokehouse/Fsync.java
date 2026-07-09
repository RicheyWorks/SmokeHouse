package io.github.richeyworks.smokehouse;

/**
 * Durability dial for the segment log (ADR D3: the log IS the WAL — this is the only fsync that
 * exists). None of these can corrupt the store — a torn tail is truncated by CRC at recovery;
 * the dial only chooses how many <em>acknowledged</em> recent appends a power loss may cost.
 */
public enum Fsync {

    /** {@code force} after every append: nothing acknowledged is ever lost; slowest. */
    ALWAYS,

    /**
     * Group fsync on a daemon every {@code fsyncIntervalMillis} (option; default 50 ms): the
     * honest middle — bounded loss window, near-OS throughput. <b>The default.</b>
     */
    INTERVAL,

    /** Let the OS page cache decide: fastest; a crash loses recently-acknowledged appends. */
    OS
}
