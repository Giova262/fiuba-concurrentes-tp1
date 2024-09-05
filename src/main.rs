use rayon::prelude::*;  

fn main() {  
    let numbers: Vec<i32> = (1..=10).collect();  

    // Fork  
    let results: Vec<i32> = numbers.par_iter()  
        .map(|&x| x * 2) // Cada tarea se ejecuta en paralelo  
        .collect();  

    // Join  
    println!("{:?}", results);  
}