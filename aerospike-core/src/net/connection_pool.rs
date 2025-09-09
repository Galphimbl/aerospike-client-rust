// Copyright 2015-2018 Aerospike, Inc.
//
// Portions may be licensed to Aerospike, Inc. under one or more contributor
// license agreements.
//
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License. You may obtain a copy of
// the License at http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// License for the specific language governing permissions and limitations under
// the License.

use std::ops::{Deref, DerefMut, Drop};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use crate::errors::{Error, Result};
use crate::net::{Connection, Host};
use crate::policy::ClientPolicy;
use futures::executor::block_on;
use futures::lock::Mutex;
use std::collections::VecDeque;
use std::time::Duration;

#[derive(Debug)]
pub struct IdleConnection(Connection);

#[derive(Debug)]
pub struct QueueInternals {
    connections: VecDeque<IdleConnection>,
    num_conns: usize,
}

#[derive(Debug)]
pub struct SharedQueue {
    internals: Mutex<QueueInternals>,
    capacity: usize,
    host: Host,
    policy: ClientPolicy,
}

#[derive(Debug)]
pub struct Queue(Arc<SharedQueue>);

impl Queue {
    pub fn with_capacity(capacity: usize, host: Host, policy: ClientPolicy) -> Self {
        let internals = QueueInternals {
            connections: VecDeque::with_capacity(capacity),
            num_conns: 0,
        };
        let shared = SharedQueue {
            internals: Mutex::new(internals),
            capacity,
            host,
            policy,
        };
        Queue(Arc::new(shared))
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        let mut internals = self.0.internals.lock().await;
        let connection;
        loop {
            if let Some(IdleConnection(mut conn)) = internals.connections.pop_front() {
                if conn.is_idle() {
                    internals.num_conns -= 1;
                    conn.close().await;
                    continue;
                }
                connection = conn;
                break;
            }

            if internals.num_conns >= self.0.capacity {
                return Err(Error::NoMoreConnections);
            }

            internals.num_conns += 1;

            // Free the lock to prevent deadlocking
            drop(internals);

            let conn = aerospike_rt::timeout(
                Duration::from_secs(5),
                Connection::new(&self.0.host.address(), &self.0.policy),
            )
            .await;

            let Ok(Ok(conn)) = conn else {
                let mut internals = self.0.internals.lock().await;
                internals.num_conns -= 1;
                drop(internals);
                return Err(Error::Connection(
                    "Could not open network connection".to_string(),
                ));
            };

            connection = conn;
            break;
        }

        Ok(PooledConnection {
            queue: self.clone(),
            conn: Some(connection),
            dirty: false,
            must_close: false,
        })
    }

    pub async fn put_back(&self, mut conn: Connection) {
        let mut internals = self.0.internals.lock().await;
        if internals.num_conns < self.0.capacity {
            internals.connections.push_back(IdleConnection(conn));
        } else {
            conn.close().await;
            internals.num_conns -= 1;
        }
    }

    pub async fn drop_conn(&self, mut conn: Connection) {
        {
            let mut internals = self.0.internals.lock().await;
            internals.num_conns -= 1;
        }
        conn.close().await;
    }

    pub async fn clear(&mut self) {
        let mut internals = self.0.internals.lock().await;
        for mut conn in internals.connections.drain(..) {
            conn.0.close().await;
        }
        internals.num_conns = 0;
    }
}

impl Clone for Queue {
    fn clone(&self) -> Self {
        Queue(self.0.clone())
    }
}

#[derive(Debug)]
pub struct ConnectionPool {
    pub num_queues: usize,
    pub queues: Vec<Queue>,
    pub queue_counter: AtomicUsize,
}

impl ConnectionPool {
    pub fn new(host: Host, policy: ClientPolicy) -> Self {
        let num_conns = policy.max_conns_per_node;
        let num_queues = policy.conn_pools_per_node;
        let queues = ConnectionPool::initialize_queues(num_conns, num_queues, host, policy);
        ConnectionPool {
            num_queues,
            queues,
            queue_counter: AtomicUsize::default(),
        }
    }

    fn initialize_queues(
        num_conns: usize,
        num_queues: usize,
        host: Host,
        policy: ClientPolicy,
    ) -> Vec<Queue> {
        let max = num_conns / num_queues;
        let mut rem = num_conns % num_queues;
        let mut queues = Vec::with_capacity(num_queues);
        for _ in 0..num_queues {
            let mut capacity = max;
            if rem > 0 {
                capacity += 1;
                rem -= 1;
            }
            queues.push(Queue::with_capacity(capacity, host.clone(), policy.clone()));
        }
        queues
    }

    pub async fn get(&self) -> Result<PooledConnection> {
        if self.num_queues == 1 {
            self.queues[0].get().await
        } else {
            let mut attempts = self.num_queues;
            loop {
                let i = self.queue_counter.fetch_add(1, Ordering::Relaxed);
                let connection = self.queues[i % self.num_queues].get().await;
                if let Err(Error::NoMoreConnections) = connection {
                    attempts -= 1;
                    if attempts > 0 {
                        continue;
                    }
                }
                return connection;
            }
        }
    }

    pub async fn close(&mut self) {
        for mut queue in self.queues.drain(..) {
            queue.clear().await;
        }
    }
}

#[derive(Debug)]
pub struct PooledConnection {
    queue: Queue,
    pub conn: Option<Connection>,
    dirty: bool,      // request started but frame not fully drained
    must_close: bool, // set when dirty at guard drop or on any parse/io error
}

impl PooledConnection {
    pub fn invalidate(mut self) {
        let conn = self.conn.take().unwrap();
        let queue = self.queue.clone();
        aerospike_rt::spawn(async move {
            queue.drop_conn(conn).await;
        });
    }
    // Call at the start of every command
    pub fn guard(&mut self) -> ConnGuard<'_> {
        self.dirty = true;
        ConnGuard { pc: self }
    }
    // Call once the full frame (header+body) is consumed successfully
    pub fn mark_clean(&mut self) {
        self.dirty = false;
    }
    // Call on any io/parse error you can't prove left us at a frame boundary
    pub fn poison(&mut self) {
        self.must_close = true;
    }
}
pub struct ConnGuard<'a> {
    pc: &'a mut PooledConnection,
}
impl ConnGuard<'_> {
    pub fn conn(&mut self) -> &mut PooledConnection {
        &mut self.pc
    }
    pub fn mark_clean(&mut self) {
        self.pc.mark_clean();
    }
    // Call on any io/parse error you can't prove left us at a frame boundary
    pub fn poison(&mut self) {
        self.pc.poison();
    }
}
impl Drop for ConnGuard<'_> {
    fn drop(&mut self) {
        // If the command future was cancelled/timed out, we never called mark_clean()
        if self.pc.dirty {
            self.pc.must_close = true; // ensure socket won't be returned to pool
        }
    }
}

impl Drop for PooledConnection {
    // fn drop(&mut self) {
    //     if let Some(conn) = self.conn.take() {
    //         block_on(self.queue.put_back(conn));
    //     }
    // }
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            // Never block in Drop. Spawn the async put/close.
            let queue = self.queue.clone();
            if self.must_close {
                // close + decrement pool count
                aerospike_rt::spawn(async move {
                    queue.drop_conn(conn).await;
                });
            } else {
                aerospike_rt::spawn(async move {
                    queue.put_back(conn).await;
                });
            }
        }
    }
}

impl Deref for PooledConnection {
    type Target = Connection;

    fn deref(&self) -> &Connection {
        self.conn.as_ref().unwrap()
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut Connection {
        self.conn.as_mut().unwrap()
    }
}
