use csv::ReaderBuilder;
use serde::Deserialize;
use std::error::Error;

#[derive(Debug, Deserialize)]
struct Record {
    field1: String,
    field2: String,
    field3: u32,
}

fn main() -> Result<(), Box<dyn Error>> {
    // Open the CSV file
    let mut rdr = ReaderBuilder::new()
        .has_headers(true) // Set to false if your CSV doesn't have headers
        .from_path("data.csv")?;

    // Iterate over each record
    for result in rdr.deserialize() {
        let record: Record = result?;
        println!("{:?}", record);
    }

    Ok(())
}

// fn main() {
//     println!("Hello, world!");

//     let mut rdr = ReaderBuilder::new()
//         .has_headers(true) // Set to false if your CSV doesn't have headers
//         .from_path("data.csv");

//     // Iterate over each record
//     for result in rdr.deserialize() {
//         let record: Record = result;
//         println!("{:?}", record);
//     }
// }
