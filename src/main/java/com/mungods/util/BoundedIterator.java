package com.mungods.util;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Implements a bounded iterator which only returns a maximum number of element from an underlying iterator
 * starting at a given offset.
 *
 * @param <T>  element type
 */
public class BoundedIterator<T> implements Iterator<T> {
    private final Iterator<T> iterator;
    private final long offset;
    private final long max;
    private int pos;
    private T next;

    /**
     * Create a new bounded iterator with a given offset and maximum
     *
     * @param offset  offset to start iteration at. Must be non negative
     * @param max  maximum elements this iterator should return. Set to -1 for all
     * @param iterator  the underlying iterator
     * @throws  IllegalArgumentException  if offset is negative
     */
    public BoundedIterator(long offset, long max, Iterator<T> iterator) {
        if (offset < 0) {
            throw new IllegalArgumentException("Offset must not be negative");
        }

        this.iterator = iterator;
        this.offset = offset;
        this.max = max;
    }

    /**
     * Factory for creating a bounded iterator.
     * @see #BoundedIterator(long, long, java.util.Iterator)
     *
     * @param offset  offset to start iteration at. Must be non negative
     * @param max  maximum elements this iterator should return. Set to -1 for all
     * @param iterator  the underlying iterator
     * @param <T>  element type
     * @return  an iterator which only returns the elements in the given bounds
     */
    public static <T> Iterator<T> create(long offset, long max, Iterator<T> iterator) {
        if (offset == 0 && max == -1) {
            return iterator;
        }
        else {
            return new BoundedIterator<T>(offset, max, iterator);
        }
    }

    public boolean hasNext() {
        if (next == null) {
            fetchNext();
        }

        return next != null;
    }

    public T next() {
        if (!hasNext()) {
            throw new NoSuchElementException();
        }
        return consumeNext();
    }

    public void remove() {
        throw new UnsupportedOperationException();
    }

    //------------------------------------------< private >---

    private void fetchNext() {
        for (; pos < offset && iterator.hasNext(); pos++) {
            next = iterator.next();
        }

        if (pos < offset || !iterator.hasNext() || max >= 0 && pos - offset + 1 > max) {
            next = null;
        }
        else {
            next = iterator.next();
            pos++;
        }

    }

    private T consumeNext() {
        T element = next;
        next = null;
        return element;
    }
}