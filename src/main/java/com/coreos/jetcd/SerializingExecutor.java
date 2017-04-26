/* ***************************************************************** */
/*                                                                   */
/* IBM Confidential                                                  */
/* OCO Source Materials                                              */
/*                                                                   */
/* (C) Copyright IBM Corp. 2001, 2017                                */
/*                                                                   */
/* The source code for this program is not published or otherwise    */
/* divested of its trade secrets, irrespective of what has been      */
/* deposited with the U. S. Copyright Office                         */
/*                                                                   */
/* US Government Users Restricted Rights - Use, duplication or       */
/* disclosure restricted by GSA ADP Schedule Contract with IBM Corp. */
/*                                                                   */
/* ***************************************************************** */

package com.coreos.jetcd;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Delegates submitted tasks to the shared threadpool but ensures they
 * are executed in order and in serial
 */
public class SerializingExecutor implements Executor {
	private final Executor sharedPool;
	private final Queue<Runnable> workQueue;
	private final boolean bounded;
	private volatile boolean scheduled = false;
	
	public SerializingExecutor(Executor parentPool) {
	    this(parentPool, 0);
	}
	
	public SerializingExecutor(Executor parentPool, int capacity) {
        if(parentPool == null) throw new NullPointerException();
        this.sharedPool = parentPool;
        this.bounded = capacity > 0;
        this.workQueue = bounded ? new ArrayBlockingQueue<>(capacity)
                : new ConcurrentLinkedQueue<>();
    }
	
	protected void logTaskUncheckedException(Throwable t) {
		t.printStackTrace();
	}
	
	@SuppressWarnings("serial")
    class TaskRun extends ReentrantLock implements Runnable {
	    @Override public void run() {
            try {
                for(;;) {
                    Queue<Runnable> wq = workQueue;
                    Runnable next; // note "this" lock is on Runner instance
                    if((next = wq.poll()) == null) {
                        lock();
                        try {
                            scheduled = false;
                            if((next = wq.poll()) == null) return;
                            scheduled = true;
                        } finally {
                            unlock();
                        }
                    }
                    try {
                        next.run();
                    } catch(RuntimeException e) {
                        logTaskUncheckedException(e);
                    }
                }
            } catch(Throwable t) {
                dispatch();
                logTaskUncheckedException(t);
                throw t;
            }
        }
	}
	
	private final TaskRun runner = new TaskRun();
	
	@Override
	public void execute(Runnable command) {
	    if(bounded) try {
            ((ArrayBlockingQueue<Runnable>)workQueue).put(command);
        } catch (InterruptedException e) {
            throw new RuntimeException(e); //TODO TBD
        }
	    else workQueue.offer(command);
	        
		if(!scheduled) {
			boolean doit = false;
			runner.lock();
			try {
				if(!scheduled) {
					scheduled = true;
					doit = true;
				}
			} finally {
			    runner.unlock();
			}
			if(doit) dispatch();
		}
	}
	
	private void dispatch() {
		boolean ok = false;
		try {
			sharedPool.execute(runner);
			ok = true;
		} finally {
			if(!ok) {
			    runner.lock();
			    try {
				scheduled = false; // bad situation
			    } finally {
			        runner.unlock();
			    }
			}
		}
	}

}
