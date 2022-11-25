/*
 * Title:        CloudSim Toolkit
 * Description:  CloudSim (Cloud Simulation) Toolkit for Modeling and Simulation of Clouds
 * Licence:      GPL - http://www.gnu.org/copyleft/gpl.html
 *
 * Copyright (c) 2009-2012, The University of Melbourne, Australia
 */

package org.cloudbus.cloudsim.core;

import java.util.Collection;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * This class implements the future event queue used by {@link Simulation}. The event queue uses a
 * {@link TreeSet} in order to store the events.
 * 
 * @author Marcos Dias de Assuncao
 * @since CloudSim Toolkit 1.0
 * @see Simulation
 * @see java.util.TreeSet
 */
public class FutureQueue {

	class MyComparator implements Comparator<SimEvent> {
		
//		 * compare - returns 1 to put last, return -1 to put top , 0 no action - used for deletion
//			"SimEvent e1" is the new item for insert ,new item is smmall then then top(-1)
			
		    @Override public int compare(SimEvent e1, SimEvent e2)
		    {
//		    	System.out.println("compare");
				if (e1 == null) {
					return 1;
				} else if (e2.eventTime() < e1.eventTime()) {
					return 1;
				} else if (e2.eventTime() > e1.eventTime()) {
					return -1;				
				} else if (e2.getSerial() < e1.getSerial()) {
					return 1;				
				} else if (e2.getSerial() > e1.getSerial()) {
					return -1;	
					
				} else if (e2 == e1) {
					return 0;
					
				} else {
					return 1;
				}
		    }
		}


	
	
	/** The sorted set. */
	private final SortedSet<SimEvent> sortedSet = new TreeSet<SimEvent>(new MyComparator());

	/** The serial. */
	private long serial = 0;

	/**
	 * Add a new event to the queue. Adding a new event to the queue preserves the temporal order of
	 * the events in the queue.
	 * 
	 * @param newEvent The event to be put in the queue.
	 */
	public void addEvent(SimEvent newEvent) {
		newEvent.setSerial(serial++);
		sortedSet.add(newEvent);
	}

	/**
	 * Add a new event to the head of the queue.
	 * 
	 * @param newEvent The event to be put in the queue.
	 */
	public void addEventFirst(SimEvent newEvent) {
		newEvent.setSerial(0);
		sortedSet.add(newEvent);
	}

	/**
	 * Returns an iterator to the queue.
	 * 
	 * @return the iterator
	 */
	public Iterator<SimEvent> iterator() {
		return sortedSet.iterator();
	}

	/**
	 * Returns the size of this event queue.
	 * 
	 * @return the size
	 */
	public int size() {
		return sortedSet.size();
	}

	/**
	 * Removes the event from the queue.
	 * 
	 * @param event the event
	 * @return true, if successful
	 */
	public boolean remove(SimEvent event) {
		return sortedSet.remove(event);
	}

	/**
	 * Removes all the events from the queue.
	 * 
	 * @param events the events
	 * @return true, if successful
	 */
	public boolean removeAll(Collection<SimEvent> events) {
		return sortedSet.removeAll(events);
	}

	/**
	 * Clears the queue.
	 */
	public void clear() {
		sortedSet.clear();
	}

}