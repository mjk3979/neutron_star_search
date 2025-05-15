use std::env;
use std::fs::File;
use neutron_route_finder::{read_neutron_stars_bincode, write_indexed_file};
use flate2::write::{GzEncoder};
use flate2::Compression;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Need input and output");
    }
    let input_file = &args[1];
    let output_file = &args[2];
    let neutrons = read_neutron_stars_bincode(input_file);
    write_indexed_file(&neutrons, output_file).unwrap();
}
