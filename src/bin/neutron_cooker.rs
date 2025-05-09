use std::env;
use std::fs::File;
use neutron_route_finder::{make_neutron_star_systems,read_star_systems_bincode};
use flate2::write::{GzEncoder};
use flate2::Compression;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 3 {
        panic!("Need input and output");
    }
    let input_file = &args[1];
    let output_file = &args[2];
    let systems = read_star_systems_bincode(input_file, |s| s.is_neutron);
    let neutrons = make_neutron_star_systems(&systems, 400.0);

    let mut out_f = File::create(output_file).unwrap();
    let mut compressed_out = GzEncoder::new(out_f, Compression::best());
    bincode::encode_into_std_write(neutrons, &mut compressed_out, bincode::config::standard()).unwrap();
}
