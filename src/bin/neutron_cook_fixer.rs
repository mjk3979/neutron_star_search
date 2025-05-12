use std::env;
use std::fs::File;
use neutron_route_finder::{read_neutron_stars_bincode,read_star_systems_bincode};
use flate2::write::{GzEncoder};
use flate2::Compression;

fn main() {
    let mut args: Vec<String> = env::args().collect();
    if args.len() < 4 {
        panic!("Need two inputs and output");
    }
    let input_file = &args[1];
    let input_file2 = &args[2];
    let output_file = &args[3];
    let systems = read_star_systems_bincode(input_file, |_| true);
    println!("Read systems");
    let n_idxs: Vec<u32> = systems.into_iter().enumerate().filter_map(|(i, s)| if s.is_neutron {Some(i as u32)} else {None}).collect();
    let mut neutrons = read_neutron_stars_bincode(input_file2);
    println!("Read neutrons");
    if n_idxs.len() != neutrons.len() {
        panic!("List lengths don't match");
    }
    println!("Fixing...");
    for (n_idx, n_system) in n_idxs.into_iter().zip(neutrons.iter_mut()) {
        n_system.idx = n_idx;
    }
    println!("Writing...");
    let mut out_f = File::create(output_file).unwrap();
    let mut buf_out = std::io::BufWriter::new(out_f);
    //let mut compressed_out = GzEncoder::new(out_f, Compression::best());
    bincode::encode_into_std_write(neutrons, &mut buf_out, bincode::config::standard()).unwrap();
}
