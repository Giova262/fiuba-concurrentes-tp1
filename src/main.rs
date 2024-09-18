use csv::Reader;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::env;
use std::error::Error;

use std::fs::File;
use std::io::Write;
use std::path::Path;

use serde::Deserialize;
use serde_json::{json, Value};

#[derive(Debug, Deserialize)]
struct Record {
    killed_by: String,
}

fn add_element_to_json(json_obj: &mut Value, key: &str, value: Value) {
    if let Value::Object(ref mut map) = json_obj {
        map.insert(key.to_string(), value);
    }
}

fn create_json_file(output_path: &str, json_data: &str) -> std::io::Result<()> {
    let path = Path::new(output_path);
    let mut file = File::create(&path)?;
    file.write_all(json_data.as_bytes())?;
    println!("JSON written to: {}", output_path);
    Ok(())
}

fn read_and_count_kills_parallel(
    input_path: String,
    output_file_name: String,
) -> Result<(), Box<dyn Error>> {
    // ARMO EL ARCHIVO
    let mut json_obj = json!({
        "padron": "93075",
    });

    let mut top_killers = json!({});
    let mut top_weapons = json!({});
    
    let mut rdr = Reader::from_path(input_path)?;

    let records: Vec<Record> = rdr.deserialize().filter_map(Result::ok).collect();

    let kill_counts: HashMap<String, u32> = records
        .par_iter() // Iteramos en paralelo usando Rayon
        .map(|record| (record.killed_by.clone(), 1)) // Mapeamos cada arma a (arma, 1)
        .fold(HashMap::new, |mut acc, (weapon, count)| {
            *acc.entry(weapon).or_insert(0) += count;
            acc
        })
        .reduce(
            || HashMap::new(),
            |mut acc, map| {
                for (weapon, count) in map {
                    *acc.entry(weapon).or_insert(0) += count;
                }
                acc
            },
        );

    let mut kills_vec: Vec<(String, u32)> = kill_counts.into_iter().collect();
    kills_vec.par_sort_by(|a, b| {
        b.1.cmp(&a.1).then(a.0.cmp(&b.0)) // En caso de empate, ordenar por nombre (alfabéticamente)
    });

    // Me quedo con los primeros 10 resultados
    let top_10 = &kills_vec[..10.min(kills_vec.len())];

    for (weapon, count) in top_10 {
        add_element_to_json(
            &mut top_weapons,
            weapon,
            json!({
                "deaths_percentage": count,
                "average_distance": count,
                "cantidad_muertes": count,
            }),
        );
    }

    add_element_to_json(&mut json_obj, "top_killers", top_killers);
    add_element_to_json(&mut json_obj, "top_weapons", top_weapons);

    let json_data = serde_json::to_string_pretty(&json_obj).expect("Error serializing to JSON");

    if let Err(e) = create_json_file(output_file_name.as_str(), &json_data) {
        eprintln!("Error creating JSON file: {}", e);
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
