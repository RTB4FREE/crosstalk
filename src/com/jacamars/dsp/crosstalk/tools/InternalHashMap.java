package com.jacamars.dsp.crosstalk.tools;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Create an internal hashmap that can be 'iterated' without an iterator. The Campaign class creates a huge amount
 * of clutter on the Heap with its iterator
 * @author Ben M. Faul
 *
 * @param <K> Object. The key.
 * @param <V> Object. The value.
 */
public class InternalHashMap<K, V> extends HashMap {
	/** Keep a list of the keys, so we can use a simple iterator on it */
	
	List<K> keys = new ArrayList();
	
	@Override
	public Object put(Object key, Object value) {
		keys.add((K)key);
		return super.put(key, value);
	}
	
	@Override 
	public Object remove(Object key) {
		keys.remove(key);
		return super.remove(key);
	}
	
	/**
	 * Get the key key at i.
	 * @param i int. The key's index.
	 * @return V. The value, or null if indexed too far.
	 */
	public K getKey(int i) {
		if (i >= keys.size())
			return null;
		return (K)keys.get(i);
	}
	
	/**
	 * Return the next object, indexed by the next key.
	 * @param i int. The index of the next key.
	 * @return V. The object at that location's key entry.
	 */
	public V getNext(int i) {
		if (i >= keys.size())
			return null;
		return (V)get(keys.get(i));
	}
}
