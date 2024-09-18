use csv::Reader;
use rayon::prelude::*;
use rayon::ThreadPoolBuilder;
use std::collections::HashMap;
use std::env;
use std::error::Error;

use std::fs;
use std::fs::File;
use std::io::Write;

use serde::Deserialize;
use serde_json::{json, Value};

use std::path::Path;

#[derive(Debug, Deserialize)]
struct Record {
    killed_by: String,
    killer_name: String,
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
    println!("Archivo de Salida creado exitosamente en: {}", output_path);
    Ok(())
}

fn read_and_count_kills_parallel(input_path: String) -> Result<(), Box<dyn Error>> {
    let mut json_salida = json!({
        "padron": "93075",
    });

    let mut json_top_killers = json!({});
    let mut json_top_weapons = json!({});

    let mut rdr = Reader::from_path(input_path)?;

    let records: Vec<Record> = rdr.deserialize().filter_map(Result::ok).collect();

    // TOP WEAPONS
    let kill_counts: HashMap<String, u32> = records
        .par_iter() // Iteramos en paralelo usando Rayon
        .map(|record| (record.killed_by.clone(), 1))
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
    kills_vec.par_sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    let top_10 = &kills_vec[..10.min(kills_vec.len())];

    for (weapon, count) in top_10 {
        add_element_to_json(
            &mut json_top_weapons,
            weapon,
            json!({
                "deaths_percentage": count,
                "average_distance": count,
                "cantidad_muertes": count,
            }),
        );
    }

    // TOP KILLER
    // let mut deaths_count: HashMap<String, u32> = HashMap::new();

    let deaths_count: HashMap<String, u32> = records
        .par_iter() // Iteramos en paralelo usando Rayon
        .map(|record| (record.killer_name.clone(), 1))
        .fold(HashMap::new, |mut acc, (killer, count)| {
            *acc.entry(killer).or_insert(0) += count;
            acc
        })
        .reduce(
            || HashMap::new(),
            |mut acc, map| {
                for (killer, count) in map {
                    *acc.entry(killer).or_insert(0) += count;
                }
                acc
            },
        );

    //   records.into_par_iter().for_each(|record| {
    //       let mut map = deaths_count.clone();
    //       *map.entry(record.killer_name).or_insert(0) += record.deaths;
    //       deaths_count = map;
    //   });

    let mut deaths_count_vec: Vec<(String, u32)> = deaths_count.into_iter().collect();
    deaths_count_vec.par_sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));

    let top_deaths_10 = &deaths_count_vec[..10.min(deaths_count_vec.len())];

    for (killer, count) in top_deaths_10 {
        add_element_to_json(
            &mut json_top_killers,
            killer,
            json!({
                "deaths": count,
                "weapons_percentage": count,
            }),
        );
    }

    // PREPARO LOS DATOS PARA EL ARCHIVO FINAL QUE SE EXPORTA00
    add_element_to_json(&mut json_salida, "json_top_killers", json_top_killers);
    add_element_to_json(&mut json_salida, "json_top_weapons", json_top_weapons);

    let json_data = serde_json::to_string_pretty(&json_salida).expect("Error serializing to JSON");

    let args: Vec<String> = env::args().collect();
    let output_file_name: String = args[3].parse().expect("Error al parse");

    if let Err(e) = create_json_file(output_file_name.as_str(), &json_data) {
        eprintln!("Error creating JSON file: {}", e);
    }

    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    if args.len() < 4 {
        eprintln!("La cantidad de argumentos no es valida, utilice el siguiente formado: cargo run <input-path> <num-threads> <output-file-name>\n");
        std::process::exit(1);
    }

    let threads: usize = args[2].parse().expect("Error al parse");

    match ThreadPoolBuilder::new().num_threads(threads).build_global() {
        Ok(_) => {}
        Err(e) => eprintln!("Error al crear ThreadPool: {}", e),
    }

    let input_path = &args[1];
    let path = Path::new(input_path);

    // Check if the path is a directory
    if !path.is_dir() {
        eprintln!("El del directorio no existe!");
        std::process::exit(1);
    }

    for entry in fs::read_dir(path)? {
        let entry = entry?;
        let file_path = entry.path();

        if file_path.is_file() && file_path.extension().and_then(|s| s.to_str()) == Some("csv") {
            println!("Processing file: {:?}", file_path);
            let file_path_str = file_path.to_str().ok_or("Invalid path")?.to_string();
            if let Err(err) = read_and_count_kills_parallel(file_path_str) {
                println!("Error: {}", err);
            }
        }
    }

    Ok(())
}
