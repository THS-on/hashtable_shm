use std::{env, process::ExitCode, str::FromStr, sync::Arc, thread, time};

use thiserror::Error;

use hashtable_shm::shm_ipc::{self, Request};

#[derive(Error, Debug)]
pub enum ClientError {
    #[error("Parser failed {0}")]
    ParserFailed(String),

    #[error("Arguments missing")]
    ArgumentsMissing,

    #[error("Unexpected Token: {0}")]
    UnexpectedToken(String),

    #[error("Parser Error: {0}")]
    ParserError(<u32 as FromStr>::Err),
}

// Key and Value type for hashtable and buffer
type TK = u32;
type TV = u32;

#[derive(Clone, Debug)]
pub enum Operation {
    Read { key: TK },
    Insert { key: TK, value: TV },
    Delete { key: TV },
}

struct Args {
    client_id: String,
    operations: Vec<Operation>,
}

impl Args {
    fn parse() -> Result<Self, ClientError> {
        let args: Vec<String> = env::args().collect();

        let mut it = args.iter().peekable();
        // Skip first as this is the program name
        it.next().ok_or(ClientError::ArgumentsMissing)?;

        let client_id = format!(
            "hashtable-{}",
            it.next().ok_or(ClientError::ArgumentsMissing)?
        );

        let mut operations: Vec<_> = vec![];

        while let Some(token) = it.next() {
            match token.as_str() {
                "insert" => {
                    operations.push(Operation::Insert {
                        key: it
                            .next()
                            .ok_or(ClientError::ArgumentsMissing)?
                            .parse()
                            .map_err(ClientError::ParserError)?,
                        value: it
                            .next()
                            .ok_or(ClientError::ArgumentsMissing)?
                            .parse()
                            .map_err(ClientError::ParserError)?,
                    });
                }
                "delete" => {
                    operations.push(Operation::Delete {
                        key: it
                            .next()
                            .ok_or(ClientError::ArgumentsMissing)?
                            .parse()
                            .map_err(ClientError::ParserError)?,
                    });
                }
                "read" => {
                    operations.push(Operation::Read {
                        key: it
                            .next()
                            .ok_or(ClientError::ArgumentsMissing)?
                            .parse()
                            .map_err(ClientError::ParserError)?,
                    });
                }
                e => return Err(ClientError::UnexpectedToken(e.to_string())),
            }
        }

        Ok(Self {
            client_id,
            operations,
        })
    }
}

fn main() -> ExitCode {
    let args = match Args::parse() {
        Ok(args) => args,
        Err(e) => {
            eprintln!("Failed to parse arguments: {e}");
            return ExitCode::FAILURE;
        }
    };
    let ipc_client: Arc<shm_ipc::ShmQueue<u32, u32>> =
        match shm_ipc::ShmQueue::new(&args.client_id, false) {
            Ok(client) => Arc::new(client),
            Err(e) => {
                eprintln!("Failed to connect to shared memory: {e}");
                return ExitCode::FAILURE;
            }
        };

    let ipc_read = ipc_client.clone();
    let count = args.operations.len();
    let handle = thread::spawn(move || {
        for _ in 0..(count) {
            match ipc_read.response_get() {
                Ok(response) => match response.error {
                    true => {
                        eprintln!("Failed to do the given operation");
                    }
                    false => {
                        if response.operation == shm_ipc::Operation::Read {
                            println!("Key: {}, Value: {}", response.key, response.val)
                        }
                    }
                },
                Err(_) => {
                    eprintln!("Failed to get response back from server");
                }
            };
        }
    });

    let mut exit_code = ExitCode::SUCCESS;

    for (counter, operation) in args.operations.iter().enumerate() {
        let request: Request<u32, u32> = match operation {
            Operation::Read { key } => Request {
                operation: shm_ipc::Operation::Read,
                key: *key,
                val: 0,
                counter,
            },
            Operation::Insert { key, value } => Request {
                operation: shm_ipc::Operation::Insert,
                key: *key,
                val: *value,
                counter,
            },
            Operation::Delete { key } => Request {
                operation: shm_ipc::Operation::Delete,
                key: *key,
                val: 0,
                counter,
            },
        };

        loop {
            match ipc_client.request_put(&request) {
                Ok(_) => break,
                Err(shm_ipc::Error::BufferFull) => thread::sleep(time::Duration::from_micros(10)), // We don't have an extra lock for this, so just  wait
                Err(_) => {
                    eprintln!("Something went wrong while trying to write to buffer");
                    break;
                }
            }
        }
    }

    match handle.join() {
        Ok(_) => (),
        Err(_) => exit_code = ExitCode::FAILURE,
    }

    exit_code
}
