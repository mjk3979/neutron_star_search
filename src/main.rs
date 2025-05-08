use std::cmp::Ordering;
use std::fs::File;
use std::io::BufReader;
use std::env;
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Reverse;

type V3 = (f64, f64, f64);

#[derive(Debug)]
struct StarSystem {
    pub name: String,
    pub main_star_type: String,
    pub coords: V3,
    pub distance_from_sol: f64,
    pub is_neutron: bool,
}

fn square(a: f64) -> f64 {
    a * a
}

fn distance_v(a: &V3, b: &V3) -> f64 {
    (square(a.0 - b.0) + square(a.1 - b.1) + square(a.2 - b.2)).sqrt()
}

fn distance(a: &StarSystem, b: &StarSystem) -> f64 {
    distance_v(&a.coords, &b.coords)
}

impl Ord for StarSystem {
    fn cmp(&self, other: &Self) -> Ordering {
        self.distance_from_sol.partial_cmp(&other.distance_from_sol).unwrap()
    }
}

impl PartialOrd for StarSystem {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for StarSystem {
    fn eq(&self, other: &Self) -> bool {
        self.name == other.name
    }
}

impl Eq for StarSystem {}

#[derive(Debug, serde::Deserialize)]
struct StarSystemRecord {
    pub name: String,
    pub Coord_X: f64,
    pub Coord_Y: f64,
    pub Coord_Z: f64,
    pub mainStarType: String,
    pub d_from_sol: f64,
}

impl From<StarSystemRecord> for StarSystem {
    fn from(record: StarSystemRecord) -> StarSystem {
        let StarSystemRecord{
            name,
            Coord_X,
            Coord_Y,
            Coord_Z,
            mainStarType,
            d_from_sol,
        } = record;
        StarSystem {
            name,
            coords: (Coord_X, Coord_Y, Coord_Z),
            is_neutron: mainStarType == "Neutron Star",
            distance_from_sol: d_from_sol,
            main_star_type: mainStarType,
        }
    }
}


fn read_star_systems(filename: &str, filter: fn(&StarSystem) -> bool) -> Vec<StarSystem> {
    let f = File::open(filename).unwrap();
    let r = BufReader::new(f);
    let mut reader = csv::Reader::from_reader(r);
    let mut retval = Vec::new();
    retval.reserve(160_000_000);
    for result in reader.deserialize::<StarSystemRecord>() {
        let system: StarSystem = result.unwrap().into();
        if filter(&system) {
            retval.push(system);
        }
    }
    retval.sort();
    return retval;
}

fn neighbors(systems: &Vec<StarSystem>, system_idx: usize, mut jump_distance: f64) -> Vec<usize> {
    let system = &systems[system_idx];
    if system.is_neutron {
        jump_distance *= 4.0;
    }
    let mut retval = Vec::new();
    retval.reserve(256);
    for neighbor_idx in (system_idx+1)..systems.len() {
        let neighbor = &systems[neighbor_idx];
        if neighbor.distance_from_sol > system.distance_from_sol + jump_distance {
            break;
        }
        if distance(system, neighbor) <= jump_distance {
            retval.push(neighbor_idx);
        }
    }
    if system_idx > 0 {
        for neighbor_idx in (0..(system_idx-1)).rev() {
            let neighbor = &systems[neighbor_idx];
            if neighbor.distance_from_sol < system.distance_from_sol - jump_distance {
                break;
            }
            if distance(system, neighbor) <= jump_distance {
                retval.push(neighbor_idx);
            }
        }
    }
    return retval;
}

#[derive(Debug,Copy,Clone)]
struct HScore {
    pub jumps: i64,
    pub distance: f64,
}

impl Ord for HScore {
    fn cmp(&self, other: &Self) -> Ordering {
        self.jumps.cmp(&other.jumps).then(self.distance.partial_cmp(&other.distance).unwrap())
    }
}

impl PartialOrd for HScore {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl PartialEq for HScore {
    fn eq(&self, other: &Self) -> bool {
        self.jumps == other.jumps && self.distance == other.distance
    }
}

impl Eq for HScore {}


fn a_star(systems: &Vec<StarSystem>, start_idx: usize, goal_idx: usize, jump_distance: f64) -> Option<Vec<usize>> {
    let start = &systems[start_idx];
    let goal = &systems[goal_idx];
    let h_fn = |system_idx: usize| -> HScore {
        let system = &systems[system_idx];
        if system_idx == goal_idx {
            let jumps = 0;
            let distance = 0.0;
            HScore{jumps, distance}
        } else if systems[system_idx].is_neutron {
            let distance = distance(system, goal);
            let after_neutron_distance = distance - (jump_distance * 4.0);
            if after_neutron_distance <= 0.0 {
                return HScore{jumps: 1, distance};
            }
            let jumps = (after_neutron_distance / jump_distance).ceil() as i64 + 1;
            HScore{jumps, distance}
        } else {
            let distance = distance(system, goal);
            let jumps = (distance / jump_distance).ceil() as i64;
            HScore{jumps, distance}
        }
    };
    let mut parent: HashMap<usize, usize> = HashMap::new();
    let mut g_score = HashMap::new();
    let mut h_score = HashMap::new();
    let mut to_visit = BinaryHeap::new();
    
    let start_h_score = HScore{jumps:0, distance:0.0};
    g_score.insert(start_idx, start_h_score);
    h_score.insert(start_idx, start_h_score);
    to_visit.push(Reverse((start_h_score, start_idx)));

    let mut max_g = 0;
    while let Some(Reverse((current_h_score, current_idx))) = to_visit.pop() {
        if current_idx == goal_idx {
            break;
        }
        if current_h_score > h_score[&current_idx] {
            continue;
        }
        let cur_g = g_score[&current_idx];
        if cur_g.jumps > max_g {
            max_g = cur_g.jumps;
            //println!("Looking at range {}", max_g);
        }
        //println!("[A] Processing {} {:?}\n {:?} {:?}", systems[current_idx].name, systems[current_idx].coords, cur_g, current_h_score);
        for neighbor_idx in neighbors(systems, current_idx, jump_distance) {
            let new_g = HScore{
                jumps: cur_g.jumps+1,
                distance: cur_g.distance + distance(&systems[current_idx], &systems[neighbor_idx]),
            };
            let mut new_h = h_fn(neighbor_idx);
            new_h.jumps += new_g.jumps;
            new_h.distance += new_g.distance;
            if !h_score.contains_key(&neighbor_idx) || new_h < h_score[&neighbor_idx] {
                *h_score.entry(neighbor_idx).or_insert(new_h) = new_h;
                *g_score.entry(neighbor_idx).or_insert(new_g) = new_g;
                *parent.entry(neighbor_idx).or_insert(0) = current_idx;
                to_visit.push(Reverse((new_h, neighbor_idx)));
            }
        }
    }

    let mut path = vec![goal_idx];
    let mut path_idx = goal_idx;
    let mut good = false;
    while let Some(&next_path_idx) = parent.get(&path_idx) {
        if next_path_idx == start_idx {
            good = true;
            break;
        }
        path.push(next_path_idx);
        path_idx = next_path_idx;
    }

    if good {
        return Some(path.into_iter().rev().collect());
    }

    None
}

fn neutron_a_star(systems: &Vec<StarSystem>, start_idx: usize, goal_idx: usize, jump_distance: f64) -> Option<Vec<usize>> {
    let start = &systems[start_idx];
    let goal = &systems[goal_idx];

    let total_distance = distance(start, goal);

    let neutron_systems: Vec<usize> = systems.iter().enumerate().filter_map(|(i, s)| {
        if s.is_neutron {
            Some(i)
        } else {
            None
        }
    }).collect();

    let h_fn = |from_idx: usize, system_idx: usize| -> HScore {
        let system = &systems[system_idx];
        let from = &systems[from_idx];
        let from_distance = distance(from, system);
        let from_jumps = if from.is_neutron {
            let after_first_jump_distance = from_distance - jump_distance * 4.0;
            if after_first_jump_distance < 0.0 {
                1
            } else {
                (after_first_jump_distance / (jump_distance)).ceil() as i64 + 1
            }
        } else {
            (from_distance / jump_distance).ceil() as i64
        };
        let goal_distance = distance(system, goal);
        let goal_jumps = (goal_distance / (jump_distance * 4.0)).ceil() as i64;
        let jumps = from_jumps + goal_jumps;
        let distance = from_distance + goal_distance;
        HScore{jumps, distance}
    };


    let mut g_score = HashMap::new();
    let mut h_score = HashMap::new();
    let mut parent: HashMap<usize, usize> = HashMap::new();
    let mut to_visit = BinaryHeap::new();
    to_visit.reserve(neutron_systems.len()+1);

    let start_h_score = HScore{jumps: 0, distance: 0.0};
    g_score.insert(start_idx, start_h_score);
    h_score.insert(start_idx, start_h_score);
    
    let no_neutron_path = a_star(systems, start_idx, goal_idx, jump_distance)?;
    let no_neutron_len = no_neutron_path.len() as i64;
    println!("No neutron length: {}", no_neutron_len);
    let no_neutron_h_score = HScore{jumps: no_neutron_len, distance: 0.0};
    h_score.insert(goal_idx, no_neutron_h_score);
    parent.insert(goal_idx, start_idx);

    to_visit.push(Reverse((no_neutron_h_score, goal_idx)));
    for &n_idx in &neutron_systems {
        let h = h_fn(start_idx, n_idx);
        *h_score.entry(n_idx).or_insert(h) = h;
        to_visit.push(Reverse((h, n_idx)));
        *parent.entry(n_idx).or_insert(start_idx) = start_idx;
    }

    while let Some(Reverse((current_h_score, current_idx))) = to_visit.pop() {
        if current_idx == goal_idx {
            break;
        }
        if current_h_score > h_score[&current_idx] {
            continue;
        }


        let parent_idx = parent[&current_idx];
        let parent_s = &systems[parent_idx];
        let current = &systems[current_idx];
        let from_path_len = if parent_s.is_neutron && distance(parent_s, current) <= jump_distance * 4.0 {
            println!("    Got one!");
            1
        } else if distance(parent_s, current) <= jump_distance {
            1
        } else {
            let from_path = a_star(systems, parent_idx, current_idx, jump_distance);
            if from_path.is_none() {
                continue;
            }
            let from_path = from_path.unwrap();
            from_path.len() as i64
        };
        let from_path_distance = distance(parent_s, current);
        let parent_g_score = g_score[&parent_idx];
        let cur_g_score = HScore{jumps: parent_g_score.jumps + from_path_len, distance:parent_g_score.distance + from_path_distance};
        println!("[N] Processing {} {:?}\n {:?} {:?}", systems[current_idx].name, systems[current_idx].coords, cur_g_score, current_h_score);
        *g_score.entry(current_idx).or_insert(cur_g_score) = cur_g_score;
        let to_goal_distance = distance(current, goal);
        let to_goal_jumps = (to_goal_distance / jump_distance).ceil() as i64;
        let to_goal_h_score = HScore {
            distance: cur_g_score.distance + to_goal_distance,
            jumps: cur_g_score.jumps + to_goal_jumps,
        };
        if to_goal_h_score < h_score[&goal_idx] {
            *h_score.get_mut(&goal_idx).unwrap() = to_goal_h_score;
            to_visit.push(Reverse((to_goal_h_score, goal_idx)));
            *parent.get_mut(&goal_idx).unwrap() = current_idx;
        }


        for &neighbor_idx in &neutron_systems {
            if neighbor_idx == current_idx {
                continue;
            }
            let mut new_h_score = h_fn(current_idx, neighbor_idx);
            new_h_score.jumps += cur_g_score.jumps;
            new_h_score.distance += cur_g_score.distance;
            if new_h_score >= no_neutron_h_score {
                continue;
            }
            if !h_score.contains_key(&neighbor_idx) || new_h_score < h_score[&neighbor_idx] {
                *h_score.entry(neighbor_idx).or_insert(new_h_score) = new_h_score;
                to_visit.push(Reverse((new_h_score, neighbor_idx)));
                *parent.entry(neighbor_idx).or_insert(current_idx) = current_idx;
            }
        }
    }

    // println!("Path found, building...");
    let mut path_idx = goal_idx;
    let mut path: Vec<usize> = Vec::new();
    let mut good = false;
    while let Some(&parent_idx) = parent.get(&path_idx) {
        // println!("Finding subpath {} {}", parent_idx, path_idx);
        let subpath = a_star(systems, parent_idx, path_idx, jump_distance)?;
        path.extend(subpath.into_iter().rev());
        if parent_idx == start_idx {
            good = true;
            break;
        }
        path_idx = parent_idx;
    }

    if good {
        return Some(path.into_iter().rev().collect());
    }


    None
}

fn main() {
    let mut args = env::args();
    let systems = read_star_systems(&args.nth(1).unwrap(), |s| true);
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

    let path = neutron_a_star(&systems, start_idx, goal_idx, 61.0).unwrap();
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
