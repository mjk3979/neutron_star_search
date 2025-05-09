use std::env;
use neutron_route_finder::{read_star_systems_bincode, neutron_a_star};

fn main() {
    let mut args = env::args();
    let systems = read_star_systems_bincode(&args.nth(1).unwrap(), |s| true);
    println!("Read {} systems", systems.len());

    let start_name = "Sol";
    let goal_name = "Colonia";
    //let start_name = "NGC 2546 Sector AO-V b33-0";
    //let goal_name = "NGC 2546 Sector KH-B b17-2";
    let (start_idx, start) = systems.iter().enumerate().find(|(_, s)| s.name == start_name).unwrap();
    let (goal_idx, goal) = systems.iter().enumerate().find(|(_, s)| s.name == goal_name).unwrap();
    println!("Start: {}, Goal: {}", start_idx, goal_idx);
    //let start_idx = 0;
    //let goal_idx = {
        
    //};

    let path = neutron_a_star(&systems, start_idx, goal_idx, 61.0.into()).unwrap();
    let path_len = path.len();
    for system_idx in path {
        let system = &systems[system_idx];
        if system.is_neutron {
            println!("{} {} {:?}", system.name, system.main_star_type, system.coords);
        } else {
            println!("    {} {} {:?}", system.name, system.main_star_type, system.coords);
        }
    }
    println!("Total jumps: {}", path_len);
}
