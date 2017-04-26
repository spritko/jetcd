/* ************************************************************************ */
/*                                                                          */
/*                                                                          */
/* (C) Copyright IBM Corp. 2001, 2017                                       */
/*                                                                          */
/* Licensed under the Apache License, Version 2.0 (the "License");          */
/* you may not use this file except in compliance with the License.         */
/* You may obtain a copy of the License at                                  */
/*                                                                          */
/* http://www.apache.org/licenses/LICENSE-2.0                               */
/*                                                                          */
/* Unless required by applicable law or agreed to in writing, software      */
/* distributed under the License is distributed on an "AS IS" BASIS,        */
/* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. */
/* See the License for the specific language governing permissions and      */
/* limitations under the License.                                           */
/*                                                                          */
/* ************************************************************************ */

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
