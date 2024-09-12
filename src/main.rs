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

// ----------------------------

// use rayon::prelude::*;
// use std::error::Error;
// use std::fs::File;
// use std::io;
// use csv::ReaderBuilder;

// fn sum_csv_column(file_path: &str) -> Result<u64, io::Error> {
//     // Intentar abrir el archivo
//     let file = File::open(file_path)?;

//     // Crear un lector CSV a partir del archivo
//     let mut rdr = ReaderBuilder::new().from_reader(file);

//     // Calcular la suma de la columna
//     let sum: u64 = rdr
//         .records()
//         .filter_map(|result| {
//             match result {
//                 Ok(record) => {
//                     record.get(0)
//                         .and_then(|val| val.parse::<u64>().ok())
//                 }
//                 Err(err) => {
//                     eprintln!("Error al leer el registro: {}", err);
//                     None
//                 }
//             }
//         })
//         .sum();

//     Ok(sum)
// }

// fn main() -> Result<(), Box<dyn Error>> {
//     // Rutas a los archivos CSV
//     let file1 = "data1.csv";
//     let file2 = "data2.csv";

//     let (result1, result2) = rayon::join(
//         || sum_csv_column(file1),
//         || sum_csv_column(file2),
//     );

//     // Leer y procesar los archivos en paralelo
//     // rayon::join(|| sum_csv_column(file1), || sum_csv_column(file2));

//     // Manejar los resultados
//     // let sum1 = result1?;
//     // let sum2 = result2?;

//     // println!("La suma de la columna en {} es: {}", file1, sum1);
//     // println!("La suma de la columna en {} es: {}", file2, sum2);
//     // println!("La suma total es: {}", sum1 + sum2);

//     Ok(())
// }


// --------------


// use csv::Reader;
// use std::error::Error;

// fn read_csv() -> Result<(), Box<dyn Error>> {
//     let mut rdr = Reader::from_path("dataset/deaths/kill_match_stats_final_0.csv")?;

//     for result in rdr.records() {
//         let record = result?;
//         println!("{:?}", record);
//     }

//     Ok(())
// }

// fn main() {
//     if let Err(err) = read_csv() {
//         println!("Error reading CSV: {}", err);
//     }
// }

//--------------------

// use std::collections::HashMap;
// use std::error::Error;
// use serde::Deserialize;
// use csv::Reader;

// #[derive(Debug, Deserialize)]
// struct Record {
//     killed_by: String,
// }

// fn read_and_count_kills() -> Result<(), Box<dyn Error>> {
//     // Creamos un HashMap para contar la cantidad de muertes por arma.
//     let mut kill_counts: HashMap<String, u32> = HashMap::new();
    
//     // Abrimos el archivo CSV
//     let mut rdr = Reader::from_path("dataset/deaths/kill_match_stats_final_0.csv")?;

//     // Recorremos cada fila
//     for result in rdr.deserialize() {
//         let record: Record = result?;
        
//         // Incrementamos el contador de muertes por arma
//         *kill_counts.entry(record.killed_by).or_insert(0) += 1;
//     }

//     // Convertimos el HashMap en un vector de tuplas para ordenar
//     let mut kills_vec: Vec<(String, u32)> = kill_counts.into_iter().collect();
    
//     // Ordenamos primero por cantidad de muertes en orden descendente y luego alfabéticamente en caso de empate
//     kills_vec.sort_by(|a, b| {
//         b.1.cmp(&a.1)  // Ordenar por la cantidad de muertes (descendente)
//             .then(a.0.cmp(&b.0))  // En caso de empate, ordenar por nombre (alfabéticamente)
//     });

//     // Tomamos los primeros 10 resultados
//     let top_10 = &kills_vec[..10.min(kills_vec.len())];

//     // Mostramos el top 10
//     for (weapon, count) in top_10 {
//         println!("{}: {}", weapon, count);
//     }

//     Ok(())
// }

// fn main() {
//     if let Err(err) = read_and_count_kills() {
//         println!("Error: {}", err);
//     }
// }


use std::collections::HashMap;
use std::error::Error;
use serde::Deserialize;
use csv::Reader;
use rayon::prelude::*; // Para usar Rayon

#[derive(Debug, Deserialize)]
struct Record {
    killed_by: String,
}

fn read_and_count_kills_parallel() -> Result<(), Box<dyn Error>> {
    // Abrimos el archivo CSV
    let mut rdr = Reader::from_path("dataset/deaths/kill_match_stats_final_0.csv")?;

    // Leemos todas las filas del CSV en memoria
    let records: Vec<Record> = rdr.deserialize()
        .filter_map(Result::ok) // Ignoramos errores en las filas y tomamos las exitosas
        .collect();

    // Particionamos los datos y procesamos en paralelo
    let kill_counts: HashMap<String, u32> = records
        .par_iter() // Iteramos en paralelo usando Rayon
        .map(|record| (record.killed_by.clone(), 1)) // Mapeamos cada arma a (arma, 1)
        .fold(
            HashMap::new, // Usamos un HashMap como acumulador local para cada thread
            |mut acc, (weapon, count)| {
                *acc.entry(weapon).or_insert(0) += count; // Contamos en el acumulador local
                acc
            },
        )
        .reduce(
            || HashMap::new(), // Inicializamos el acumulador global
            |mut acc, map| {
                // Combinamos los acumuladores de cada thread
                for (weapon, count) in map {
                    *acc.entry(weapon).or_insert(0) += count;
                }
                acc
            },
        );

    // Convertimos el HashMap en un vector de tuplas para ordenar
    let mut kills_vec: Vec<(String, u32)> = kill_counts.into_iter().collect();

    // Ordenamos primero por la cantidad de muertes en orden descendente y luego alfabéticamente en caso de empate
    kills_vec.par_sort_by(|a, b| {
        b.1.cmp(&a.1)  // Ordenar por la cantidad de muertes (descendente)
            .then(a.0.cmp(&b.0))  // En caso de empate, ordenar por nombre (alfabéticamente)
    });

    // Tomamos los primeros 10 resultados
    let top_10 = &kills_vec[..10.min(kills_vec.len())];

    // Mostramos el top 10
    for (weapon, count) in top_10 {
        println!("{}: {}", weapon, count);
    }

    Ok(())
}

fn main() {
    if let Err(err) = read_and_count_kills_parallel() {
        println!("Error: {}", err);
    }
}
