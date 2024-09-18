use csv::Reader;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;

use serde::Deserialize;
use std::collections::HashMap;
use std::env;
use std::error::Error;

use std::fs::File;
use std::io::Write;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct Record {
    killed_by: String,
}

fn crear_archivo_salida(output_path: &str, contenido: &str) -> std::io::Result<()> {
    // Convertir la cadena de la ruta en un objeto Path
    let path = Path::new(output_path);

    // Crear el archivo en la ruta especificada
    let mut archivo = File::create(&path)?;

    // Escribir el contenido en el archivo
    archivo.write_all(contenido.as_bytes())?;

    println!("Archivo creado y contenido escrito en: {}", output_path);
    Ok(())
}

fn read_and_count_kills_parallel(
    input_path: String,
    output_file_name: String,
) -> Result<(), Box<dyn Error>> {
    // Abrimos el archivo CSV
    let mut rdr = Reader::from_path(input_path)?;
    //let mut rdr = Reader::from_path("dataset/deaths/kill_match_stats_final_0.csv")?;

    // Leemos todas las filas del CSV en memoria
    let records: Vec<Record> = rdr
        .deserialize()
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
        b.1.cmp(&a.1) // Ordenar por la cantidad de muertes (descendente)
            .then(a.0.cmp(&b.0)) // En caso de empate, ordenar por nombre (alfabéticamente)
    });

    // Tomamos los primeros 10 resultados
    let top_10 = &kills_vec[..10.min(kills_vec.len())];

    // Mostramos el top 10
    for (weapon, count) in top_10 {
        println!("{}: {}", weapon, count);
    }

    let contenido = "Este es el contenido del archivo de salida.";

    // Llamar a la función para crear el archivo de salida
    if let Err(e) = crear_archivo_salida(output_file_name.as_str(), contenido) {
        eprintln!("Error al crear el archivo: {}", e);
    }

    Ok(())
}

fn main() {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        eprintln!("La cantidad de argumentos no es valida, utilice el siguiente formado: cargo run <input-path> <num-threads> <output-file-name>\n");
        std::process::exit(1);
    }

    let input_path: String = args[1].parse().expect("Error al parse");
    let threads: usize = args[2].parse().expect("Error al parse");
    let output_file_name: String = args[3].parse().expect("Error al parse");

    println!("threads: {}", threads);

    match ThreadPoolBuilder::new().num_threads(threads).build_global() {
        Ok(_) => {}
        Err(e) => eprintln!("Error al crear ThreadPool: {}", e),
    }

    if let Err(err) = read_and_count_kills_parallel(input_path, output_file_name) {
        println!("Error: {}", err);
    }
}
