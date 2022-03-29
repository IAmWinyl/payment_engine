use csv::WriterBuilder;
use csv::Trim;
use serde::{Serialize,Serializer,Deserialize};
use std::process;
use std::error::Error;
use std::io;
use clap::Parser;
use rust_decimal_macros::dec;
use rust_decimal::prelude::*;
use std::collections::HashMap;

#[derive(Parser)]
struct Args {
    csv_file: String,
}

#[derive(Debug, Deserialize)]
struct Record {
    transaction_type: String,
    client_id: u16,
    amount: Decimal,
    disputed: bool,
    locked: bool,
}

#[derive(Debug, Serialize)]
struct Client {
    #[serde(rename = "client")]
    client_id: u16,
    #[serde(serialize_with = "round_serialize")]
    available: Decimal,
    #[serde(serialize_with = "round_serialize")]
    held: Decimal,
    #[serde(serialize_with = "round_serialize")]
    total: Decimal,
    locked: bool,
}

// This macro rounds the Decimal units to 4 significance places in the Bankers Rounding method
fn round_serialize<S>(x: &Decimal, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_f32(match x.round_dp(4).to_f32(){
        Some(x) => x,
        None => -1.0,
    })
}

// This function is the main logic that handles opening and reading the CSV and delegating each transaction type
fn open_and_read_csv(csv_file: String) -> Result<HashMap<u16,Client>, Box<dyn Error>> {
    let mut records = HashMap::<u32,Record>::new();
    let mut clients = HashMap::<u16,Client>::new();

    // Set up path for CSV file
    let mut path_abs = std::env::current_exe()?;
    path_abs.pop();
    path_abs.push(csv_file);

    // Set up CSV reader
    let mut rdr = match csv::ReaderBuilder::new()
                    .trim(Trim::All)
                    .from_path(&path_abs) {
                        Ok(r) => r,
                        Err(e) => {
                            println!("ERR: could not find the file in path {}",&path_abs.display());
                            process::exit(-1);
                        }
                    };


    for result in rdr.records() {
        let record = result?;

        // DEBUG
        //println!("{:?}",record);

        // Parse CSV into hashmap
        let transaction_id = record[2].parse::<u32>()?;
        let transaction_type = (record[0]).to_string();
        if (record[0]).to_string() == "deposit" || (record[0]).to_string() == "withdrawal" {
            records.insert(transaction_id.clone(), Record {
                transaction_type: transaction_type.clone(), 
                client_id: record[1].parse::<u16>()?,
                amount: record[3].parse::<Decimal>()?,
                disputed: false,
                locked: false,
            });
        }

        // Perform action type
        match transaction_type.as_str() {
            "deposit" => deposit_to_account(&mut clients, records.get(&transaction_id).unwrap()),
            "withdrawal" => withdraw_from_account(&mut clients, records.get(&transaction_id).unwrap()),
            "dispute" => submit_dispute(&mut clients, &mut records, &transaction_id, &record[1].parse::<u16>()?),
            "resolve" => resolve_dispute(&mut clients, &mut records, &transaction_id, &record[1].parse::<u16>()?),
            "chargeback" => issue_chargeback(&mut clients, &mut records, &transaction_id, &record[1].parse::<u16>()?),
            _  => {
                println!("Error while parsing CSV: Invalid transaction type.");
                process::exit(-1);
            },
        }

        // DEBUG
        //match records.get(&transaction_id) {
        //    Some(r) => println!("{:?}",r),
        //    None => println!("Entry does not exist."),
        //};

        //match clients.get(&records.get(&transaction_id).unwrap().client_id) {
        //    Some(r) => println!("{:?}",r),
        //    None => println!("Entry does not exist."),
        //};

        println!("\n\n");
    }

    Ok(clients)
}

// This function deposits money into a client's account
fn deposit_to_account(clients: &mut HashMap::<u16,Client>, record: &Record) {
    match clients.get_mut(&(record.client_id)) {
        // Add amount to client
        Some(x) => {
            if x.locked != true {
                x.available += record.amount;
                x.total += record.amount;
            }
        },
        // Create a new client if not already in list
        None => drop(clients.insert(record.client_id, Client {
                    client_id: record.client_id,
                    available: record.amount,
                    held: dec!(0),
                    total: record.amount,
                    locked: false,
                })),
    };

    // DEBUG
    println!("Deposit {:?} : {:?}",&(record.client_id),clients.get(&(record.client_id)).unwrap());
}

// This function withdraws money into a client's account
fn withdraw_from_account(clients: &mut HashMap::<u16,Client>, record: &Record) {
    match clients.get_mut(&(record.client_id)) {
        // Subtract amount from client, error if insufficient funds are available
        Some(x) => {
            if x.available > record.amount && x.locked != true {
                x.available -= record.amount;
                x.total -= record.amount;
            } else {
                println!("Error: Insufficient funds for withdrawal.");
                x.locked = true;
            }

        },
        None => (),
    };

    // DEBUG
    println!("Withdraw {:?} : {:?}",&(record.client_id),clients.get(&(record.client_id)).unwrap());
}

// This function submits a dispute onto the client and places funds from available to held
fn submit_dispute(clients: &mut HashMap::<u16,Client>, records: &mut HashMap::<u32,Record>, transaction_id: &u32, client_id: &u16) {
    // Get record associated with transaction id
    let record = match records.get_mut(transaction_id) {
        Some(x) => x,
        None => {
            println!("Error: transaction does not exist.");
            return;
        },
    };

    // Check if client id's match
    if client_id == &record.client_id {
        match record.transaction_type.as_str() {
            "deposit" => {
                match clients.get_mut(&(record.client_id)) {
                    // Check if client exists
                    Some(x) => {
                        // Check if record is already being disputed or chargeback has already occured (aka, account is locked)
                        if record.disputed == false && record.locked == false {
                            x.available -= record.amount;
                            x.held += record.amount;
                            record.disputed = true;
                        }
                        else {
                            println!("Error: Transaction is already being disputed or has already been resolved.");
                        }
                    },
                    None => println!("Error: Client {} does not exist.", &(record.client_id)),
                }
            },
            _ => println!("Error: Transaction type {} cannot be disputed.", &record.transaction_type),
        };
    }
    else {
        println!("Error: Client does not match transaction.")
    }
}

// This function resolves a record under dispute and places funds from held back to available
fn resolve_dispute(clients: &mut HashMap::<u16,Client>, records: &mut HashMap::<u32,Record>, transaction_id: &u32, client_id: &u16) {
    // Get record associated with transaction id
    let record = match records.get_mut(transaction_id) {
        Some(x) => x,
        None => {
            println!("Error: transaction does not exist.");
            return;
        },
    };

    // Check if client id's match    
    if client_id == &record.client_id {
        match record.transaction_type.as_str() {
            "deposit" => {
                // Check if client exists
                match clients.get_mut(&(record.client_id)) {
                    Some(x) => {
                        // Check if record is under dispute
                        if record.disputed == true {
                            x.available += record.amount;
                            x.held -= record.amount;
                            record.disputed = false;
                        }
                        else {
                            println!("Error: Transaction is not being disputed.");
                        }
                    },
                    None => println!("Error: Client {} does not exist.", &(record.client_id)),
                }
            },
            _ => println!("Error: Transaction type {} cannot be resolved.", &record.transaction_type),
        };
    }
    else {
        println!("Error: Client does not match transaction.")
    }
}

// This function issues a chargeback on a record by taking the disputed amount away from held and total, and locks the record and client
fn issue_chargeback(clients: &mut HashMap::<u16,Client>, records: &mut HashMap::<u32,Record>, transaction_id: &u32, client_id: &u16) {
    // Get record associated with transaction id
    let record = match records.get_mut(transaction_id) {
        Some(x) => x,
        None => {
            println!("Error: transaction does not exist.");
            return;
        },
    };

    // Check if client id's match  
    if client_id == &record.client_id {
        match record.transaction_type.as_str() {
            "deposit" => {
                // Check if client exists
                match clients.get_mut(&(record.client_id)) {
                    Some(x) => {
                        // Check if record is under dispute
                        if record.disputed == true {
                            x.total -= record.amount;
                            x.held -= record.amount;
                            x.locked = true;
                            record.disputed = false;
                            record.locked = true;
                        }
                        else {
                            println!("Error: Transaction is not being disputed.");
                        }
                    },
                    None => println!("Error: Client {} does not exist.", &(record.client_id)),
                }
            },
            _ => println!("Error: Transaction type {} cannot be resolved.", &record.transaction_type),
        };
    }
    else {
        println!("Error: Client does not match transaction.")
    }
}

//  This function writes each client data struct to stdout in the CSV format
fn write_to_csv(clients: HashMap::<u16,Client>) -> Result<(), Box<dyn Error>> {
    let mut wtr = WriterBuilder::new().from_writer(io::stdout());
    
    for (id, data) in clients.iter() {
        wtr.serialize(data);
        wtr.flush()?;
    }

    Ok(())
}

fn main() {
    let args = Args::parse();

    let clients = match open_and_read_csv(args.csv_file) {
        Ok(c) => c,
        Err(e) => {
            println!("Error while parsing CSV: {:?}", e);
            process::exit(-1);
        }
    };
    
    match write_to_csv(clients) {
        Ok(_) => (),
        Err(e) => println!("Error: {}",e),
    }
    
}
