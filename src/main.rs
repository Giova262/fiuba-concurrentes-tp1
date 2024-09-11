// use csv::ReaderBuilder;
// use serde::Deserialize;
// use std::error::Error;

// #[derive(Debug, Deserialize)]
// struct Record {
//     field1: String,
//     field2: String,
//     field3: u32,
// }

// fn main() -> Result<(), Box<dyn Error>> {
//     // Open the CSV file
//     let mut rdr = ReaderBuilder::new()
//         .has_headers(true) // Set to false if your CSV doesn't have headers
//         .from_path("data.csv")?;

//     // Iterate over each record
//     for result in rdr.deserialize() {
//         let record: Record = result?;
//         println!("{:?}", record);
//     }

//     Ok(())
// }

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

// --------------

// use rayon::prelude::*;

// fn main() {
//     let data: Vec<u64> = (1..=1000).collect();
//     let total: u64 = data.par_iter().map(|&x| x).sum();
//     println!("La suma es: {}", total);
// }

use rayon::prelude::*;
use std::error::Error;
use std::fs::File;
use std::io;
use csv::ReaderBuilder;

fn sum_csv_column(file_path: &str) -> Result<u64, io::Error> {
    // Intentar abrir el archivo
    let file = File::open(file_path)?;
    
    // Crear un lector CSV a partir del archivo
    let mut rdr = ReaderBuilder::new().from_reader(file);

    // Calcular la suma de la columna
    let sum: u64 = rdr
        .records()
        .filter_map(|result| {
            match result {
                Ok(record) => {
                    record.get(0)
                        .and_then(|val| val.parse::<u64>().ok())
                }
                Err(err) => {
                    eprintln!("Error al leer el registro: {}", err);
                    None
                }
            }
        })
        .sum();

    Ok(sum)
}

fn main() -> Result<(), Box<dyn Error>> {
    // Rutas a los archivos CSV
    let file1 = "data1.csv";
    let file2 = "data2.csv";

    let (result1, result2) = rayon::join(
        || sum_csv_column(file1),
        || sum_csv_column(file2),
    );

    // Leer y procesar los archivos en paralelo
    // rayon::join(|| sum_csv_column(file1), || sum_csv_column(file2));

    // Manejar los resultados
    // let sum1 = result1?;
    // let sum2 = result2?;

    // println!("La suma de la columna en {} es: {}", file1, sum1);
    // println!("La suma de la columna en {} es: {}", file2, sum2);
    // println!("La suma total es: {}", sum1 + sum2);

    Ok(())
}
