use std::env;
use neutron_route_finder::{neutron_a_star, IndexedFileMap, StarSystemRecord, StarSystem};

fn main() {
    let args: Vec<String> = env::args().collect();
    let systems: IndexedFileMap<StarSystemRecord> = IndexedFileMap::new(&args[1]);
    let neutron_systems = IndexedFileMap::new(&args[2]);
    println!("Read {} systems", systems.len());

    let start_name = "Sol";
    let goal_name = "Colonia";
    //let start_name = "Catun";
    //let goal_name = "Traikee IY-U c2-4";
    //let start_name = "NGC 2546 Sector AO-V b33-0";
    //let goal_name = "NGC 2546 Sector KH-B b17-2";
    let (start_idx, goal_idx) = {
        let mut start_idx = None;
        let mut goal_idx = None;
        for idx in 0..systems.len() {
            let system = systems.get(idx);
            if start_idx.is_none() && system.name == start_name {
                start_idx = Some(idx);
            }
            if goal_idx.is_none() && system.name == goal_name {
                goal_idx = Some(idx);
            }
            if start_idx.is_some() && goal_idx.is_some() {
                break;
            }
        }
        (start_idx.unwrap(), goal_idx.unwrap())
    };
                
    println!("Start: {}, Goal: {}", start_idx, goal_idx);
    //let start_idx = 0;
    //let goal_idx = {
        
    //};

    let path = neutron_a_star(&systems, &neutron_systems, start_idx, goal_idx, 63.0.into()).unwrap();
    let path_len = path.len();
    for system_idx in path {
        let system: StarSystem = systems.get(system_idx).into();
        if system.is_neutron {
            println!("{} {} {:?}", system.name, system.main_star_type, system.coords);
        } else {
            println!("    {} {} {:?}", system.name, system.main_star_type, system.coords);
        }
    }
    println!("Total jumps: {}", path_len);
}
