use std::env;
use std::fs::File;
use neutron_route_finder::{StarSystemRecord, read_star_systems_bincode, write_indexed_file};
use flate2::write::{GzEncoder};
use flate2::Compression;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Need input and output");
    }
    let input_file = &args[1];
    let output_file = &args[2];
    let systems: Vec<StarSystemRecord> = read_star_systems_bincode(input_file, |s| true).into_iter().map(|r| r.into()).collect();
    write_indexed_file(&systems, output_file).unwrap();
}
