package org.tinymq.message;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class Queue<Item> implements Iterable<Item> {

    private static final int DEFAULT_SIZE = 100;
    private static final int BATCH_SIZE = 10;
    private Item[] array;
    private int n = 0;// size of the queue
    private int first = 0;// points to the least recently added element
    private int last = 0;// points to the next empty space in the array after the last element
    private int first_offset = 0;

    /**
     * Initializes an empty queue.
     */
    public Queue() {
        array = (Item[]) new Object[DEFAULT_SIZE];
    }

    /**
     * @return true if this queue is empty; false otherwise
     */
    public boolean isEmpty() {
        return n == 0;
    }

    /**
     * @return the number of items in this queue.
     */
    public int size() {
        return n;
    }

    /**
     * enqueues the item in the queue
     */
    public synchronized void enqueue(Item item) {
        if (n >= array.length) {
            throw new RuntimeException("queue overflow");
        }

        // add the item to the next empty space after the last element
        array[last++] = item;

        //wrap around to make most use of the array
        if (last == array.length)
            last = 0;
        n++;
    }

    /**
     * @return the first item in the queue after removing
     */

    public Item dequeue() {
        Item deq = array[first];
        array[first++] = null;
        first_offset++;
        if (first == array.length)
            first = 0;
        n--;
        return deq;
    }

    /**
     * @return message at given offset
     */
    public Item getItemAtOffset(int offset) {
        if (offset < first_offset) {
            throw new NoSuchElementException("Message at offset " + offset + " has expired");
        }

        Item item = array[(first + offset - first_offset) % array.length];
        if (item == null) {
            throw new NoSuchElementException("Illegal offset value");
        }

        return item;
    }

    public Item[] getBatchAtOffset(int offset) {
        Item[] items = (Item[]) new Object[BATCH_SIZE];
        for (int i = 0; i < BATCH_SIZE; i++) {
            Item item = array[(first + offset + i - first_offset) % array.length];
            if (item == null) {
                return items;
            }
            items[i] = item;
        }
        return items;
    }

    /**
     * put the item at the given offset
     *
     * @param item
     * @param offset
     */
    public void putItemAtOffset(Item item, int offset) {
        if (offset - first_offset >= DEFAULT_SIZE) {
            throw new RuntimeException("queue overflow");
        }

        array[(first + offset - first_offset) % array.length] = item;
        n++;
    }

    public Iterator<Item> iterator() {
        return new Iterator<Item>() {
            int i = 0;

            public boolean hasNext() {
                return (i < n);

            }

            public Item next() {
                if (!hasNext())
                    throw new NoSuchElementException();
                Item item = array[(i + first) % array.length];
                i++;
                return item;
            }

            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    public String toString() {
        StringBuilder sb = new StringBuilder().append("Queue[");
        for (int i = 0; i < array.length; i++) {
            Item item = array[(i + first) % array.length];
            sb.append(item);
            sb.append(",");
        }

        return sb.append("]").toString();
    }

    public static void main(String[] args) {
        Queue<Integer> q = new Queue<>();
        q.enqueue(0);
        q.enqueue(1);
        q.putItemAtOffset(10, 10);
        q.putItemAtOffset(99, 99);
        q.dequeue();
        q.putItemAtOffset(100, 100);
        q.dequeue();
        q.putItemAtOffset(101, 101);

        System.out.println(q);

        System.out.println(q.getItemAtOffset(10));
        System.out.println(q.getItemAtOffset(101));

    }
}

