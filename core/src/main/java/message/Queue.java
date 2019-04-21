package message;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class Queue<Item> implements Iterable<Item> {

    private static final int DEFAULT_SIZE = 4;
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
    public void enqueue(Item item) {
        // resize to double size if necessary
        if (n >= array.length)
            resize(array.length * 2);

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
        if (n <= array.length / 4)
            resize(array.length / 2);
        Item deq = array[first];
        array[first++] = null;
        first_offset++;
        if (first == array.length)
            first = 0;
        n--;
        return deq;
    }

    /**
     * @return the first item in the queue
     */
    public Item peek() {
        if (isEmpty())
            throw new NoSuchElementException("Queue underflow");
        return array[first];
    }

    /**
     * @return message at given offset
     */
    public Item atOffset(int offset) {
        if (offset < first_offset) {
            throw new NoSuchElementException("Message at offset " + offset + " has expired");
        }

        if (offset >= first_offset + n) {
            throw new NoSuchElementException("Illegal offset value");
        }

        return array[(first + offset - first_offset) % array.length];
    }

    private void resize(int capacity) {
        Item[] temp = (Item[]) new Object[capacity];
        for (int i = 0; i < n; i++) {
            temp[i] = array[(first + i) % array.length];
        }

        array = temp;
        first = 0;
        last = n;
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
        for (int i = 0; i < n; i++) {
            Item item = array[(i + first) % array.length];
            sb.append(item);
            if (i != n - 1) {
                sb.append(",");
            }
        }

        return sb.append("]").toString();
    }
}

