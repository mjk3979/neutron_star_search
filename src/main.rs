use std::env;
use neutron_route_finder::{neutron_a_star, IndexedFileMap, CachedIndexedFileMap, StarSystemRecord, StarSystem, read_star_systems_bincode, VecMap};
use std::sync::{Arc, Mutex};
use rayon::prelude::*;

#[tokio::main]
async fn main() {
    let args: Vec<String> = env::args().collect();
    let systems: Vec<StarSystem> = read_star_systems_bincode(&args[1], |_| true);
    let systems = VecMap::new(systems);
    let neutron_systems = Arc::new(IndexedFileMap::new(&args[2]));
    println!("Read {} systems", systems.len());

    let start_name = "Sol";
    let goal_name = "Colonia";
    //let start_name = "Catun";
    //let goal_name = "Traikee IY-U c2-4";
    //let start_name = "NGC 2546 Sector AO-V b33-0";
    //let goal_name = "NGC 2546 Sector KH-B b17-2";
    let (start_idx, goal_idx) = {
        let mut start_idx = Arc::new(Mutex::new(None));
        let mut goal_idx = Arc::new(Mutex::new(None));
        (0..systems.len()).par_bridge().find_map_any(|idx| {
            let system = systems.get(idx);
            if system.name == start_name {
                let mut start_idx = start_idx.lock().unwrap();
                let mut goal_idx = goal_idx.lock().unwrap();
                *start_idx = Some(idx);
                if start_idx.is_some() && goal_idx.is_some() {
                    return Some(());
                }
            }
            if system.name == goal_name {
                let mut start_idx = start_idx.lock().unwrap();
                let mut goal_idx = goal_idx.lock().unwrap();
                *goal_idx = Some(idx);
                if start_idx.is_some() && goal_idx.is_some() {
                    return Some(());
                }
            }
            None
        }).unwrap();
        (start_idx.lock().unwrap().unwrap(), goal_idx.lock().unwrap().unwrap())
    };
                
    println!("Start: {}, Goal: {}", start_idx, goal_idx);
    //let start_idx = 0;
    //let goal_idx = {
        
    //};

    let path = neutron_a_star(&systems, &neutron_systems, start_idx, goal_idx, 63.0.into()).await.unwrap();
    let path_len = path.len();
    for system_idx in path {
        let system = systems.get(system_idx);
        if system.is_neutron {
            println!("{} {} {:?}", system.name, system.main_star_type, system.coords);
        } else {
            println!("    {} {} {:?}", system.name, system.main_star_type, system.coords);
        }
    }
    println!("Total jumps: {}", path_len);
}
