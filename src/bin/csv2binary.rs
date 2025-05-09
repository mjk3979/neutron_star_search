use std::env;
use std::fs::File;
use neutron_route_finder::{read_star_systems_csv, StarSystemRecord};
use flate2::write::{GzEncoder};
use flate2::Compression;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Need input and output");
    }
    let input_file = &args[1];
    let output_file = &args[2];
    let systems: Vec<StarSystemRecord> = read_star_systems_csv(input_file, |s| true).into_iter().map(|s| s.into()).collect();
    let mut out_f = File::create(output_file).unwrap();
    let mut compressed_out = GzEncoder::new(out_f, Compression::best());
    bincode::encode_into_std_write(systems, &mut compressed_out, bincode::config::standard()).unwrap();
}
