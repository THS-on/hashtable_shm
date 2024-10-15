use std::{
    process::ExitCode,
    sync::{mpsc, Arc},
    thread, time,
};

use clap::{command, Parser};

use hashtable_shm::{
    hashtable,
    shm_ipc::{self, Operation},
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    bucket_size: usize,
    clients: usize,
    threads: usize,
}

fn main() -> ExitCode {
    let args = Args::parse();
    let table: Arc<hashtable::HashTable<u32, u32>> =
        match hashtable::HashTable::new(args.bucket_size) {
            Ok(t) => Arc::new(t),
            Err(e) => {
                eprintln!("Failed to create hashtable: {}", e);
                return ExitCode::FAILURE;
            }
        };

    // Setup Ctrl-C handler with channel
    let (tx, rx) = mpsc::channel();
    let tx_handler = tx.clone();
    if ctrlc::set_handler(move || {
        tx_handler
            .send(ExitCode::SUCCESS)
            .expect("Error sending shutdown event");
    })
    .is_err()
    {
        eprintln!("Failed to setup Ctrl-C handler");
        return ExitCode::FAILURE;
    }

    let mut ipcs: Vec<_> = vec![];
    for client_id in 0..args.clients {
        let ipc = match shm_ipc::ShmQueue::new(format!("hashtable-{}", client_id).as_str(), true) {
            Ok(ipc) => Arc::new(ipc),
            Err(_) => {
                eprintln!("Failed to create shared memory for client {}", client_id);
                tx.clone()
                    .send(ExitCode::FAILURE)
                    .expect("Error sending shutdown event");
                break;
            }
        };
        ipcs.push(ipc.clone());
        for _ in 0..args.threads {
            //let name_ipc = name.clone();
            let t_table = table.clone();
            let ipc_client = ipc.clone();
            let _ = thread::spawn(move || loop {
                if let Ok(request) = ipc_client.request_get() {
                    println!("Got request: {:?}", request);

                    let response = match request.operation {
                        Operation::Read => match t_table.read(&request.key) {
                            Some(value) => shm_ipc::Response {
                                operation: Operation::Read,
                                error: false,
                                key: request.key,
                                val: value,
                                counter: request.counter,
                            },
                            None => shm_ipc::Response {
                                operation: Operation::Read,
                                error: true,
                                key: request.key,
                                val: 0,
                                counter: request.counter,
                            },
                        },
                        Operation::Insert => match t_table.add(request.key, request.val) {
                            Ok(_) => shm_ipc::Response {
                                operation: Operation::Insert,
                                error: false,
                                key: request.key,
                                val: request.val,
                                counter: request.counter,
                            },
                            Err(_) => shm_ipc::Response {
                                operation: Operation::Insert,
                                error: true,
                                key: request.key,
                                val: request.val,
                                counter: request.counter,
                            },
                        },
                        Operation::Delete => match t_table.delete(&request.key) {
                            Ok(()) => shm_ipc::Response {
                                operation: Operation::Delete,
                                error: false,
                                key: request.key,
                                val: 0,
                                counter: request.counter,
                            },
                            Err(_) => shm_ipc::Response {
                                operation: Operation::Delete,
                                error: true,
                                key: request.key,
                                val: 0,
                                counter: request.counter,
                            },
                        },
                    };
                    loop {
                        match ipc_client.response_put(&response) {
                            Ok(_) => break,
                            Err(shm_ipc::Error::BufferFull) => {
                                thread::sleep(time::Duration::from_micros(10))
                            } // We don't have an extra lock for this, so just wait
                            Err(_) => {
                                eprintln!("Something went wrong while trying to write to buffer");
                                break;
                            }
                        }
                    }
                }
            });
        }
    }

    println!("Use Ctrl-C to stop server...");
    let exit_code = rx.recv().expect("Cloud not wait for shutdown handler");
    for ipc in ipcs {
        match ipc.stop() {
            Ok(()) => (),
            Err(_) => eprint!("failed to stop ipc"),
        }
    }
    println!("Shutting down...");
    exit_code
}
