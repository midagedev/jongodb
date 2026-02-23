package org.jongodb.wire;

/**
 * Marker for OP_MSG sections.
 * The skeleton codec currently supports only a single kind 0 body section.
 */
public interface OpMsgSection {
    byte kind();
}
