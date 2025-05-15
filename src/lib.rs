use std::cmp::Ordering;
use std::fs::File;
use std::io::BufReader;
use std::collections::{HashMap, BinaryHeap};
use std::cmp::Reverse;
use fast_fp::{FF32};
use rayon::prelude::*;
use std::sync::{Arc, Mutex};
use std::io::Seek;
use memmap2::{Mmap};

pub type Float = FF32;
pub type V3 = (Float, Float, Float);

#[derive(Debug)]
pub struct StarSystem {
    pub name: String,
    pub main_star_type: String,
    pub coords: V3,
    pub distance_from_sol: Float,
    pub is_neutron: bool,
}

#[derive(Debug, Clone, bincode::Encode, bincode::Decode)]
pub struct NeutronStarSystem {
    pub idx: u32,
    pub neighbors: Box<[u32]>,
}

fn square(a: Float) -> Float {
    a * a
}

fn distance_v(a: &V3, b: &V3) -> Float {
    (square(a.0 - b.0) + square(a.1 - b.1) + square(a.2 - b.2)).sqrt()
}

pub fn distance(a: &StarSystem, b: &StarSystem) -> Float {
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

#[derive(Debug, serde::Deserialize, serde::Serialize, bincode::Encode, bincode::Decode)]
pub struct StarSystemRecord {
    pub name: String,
    pub Coord_X: f32,
    pub Coord_Y: f32,
    pub Coord_Z: f32,
    pub mainStarType: String,
    pub d_from_sol: f32,
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
            coords: (Coord_X.into(), Coord_Y.into(), Coord_Z.into()),
            is_neutron: mainStarType == "Neutron Star",
            distance_from_sol: d_from_sol.into(),
            main_star_type: mainStarType,
        }
    }
}

impl From<StarSystem> for StarSystemRecord {
    fn from(record: StarSystem) -> StarSystemRecord {
        let StarSystem {
            name,
            coords: (x, y, z),
            main_star_type,
            distance_from_sol,
            ..
        } = record;
        StarSystemRecord{
            name,
            Coord_X: x.into(),
            Coord_Y: y.into(),
            Coord_Z: z.into(),
            mainStarType: main_star_type,
            d_from_sol: distance_from_sol.into(),
        }
    }
}


pub fn read_star_systems_csv(filename: &str, filter: fn(&StarSystem) -> bool) -> Vec<StarSystem> {
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

pub fn read_star_systems_bincode(filename: &str, filter: fn(&StarSystem) -> bool) -> Vec<StarSystem> {
    let f = File::open(filename).unwrap();
    let mut gz = flate2::read::GzDecoder::new(f);
    let records: Vec<StarSystemRecord> = bincode::decode_from_std_read(&mut gz, bincode::config::standard()).unwrap();
    records.into_iter().map(|r| r.into()).filter(filter).collect()
}

pub fn read_neutron_stars_bincode(filename: &str) -> Box<[NeutronStarSystem]> {
    let f = File::open(filename).unwrap();
    //let mut gz = flate2::read::GzDecoder::new(f);
    let mut buf = BufReader::new(f);
    let records: Vec<NeutronStarSystem> = bincode::decode_from_std_read(&mut buf, bincode::config::standard()).unwrap();
    records.into_iter().collect()
}

fn neighbors(systems: &IndexedFileMap<StarSystemRecord>, system_idx: u32, mut jump_distance: Float) -> Vec<u32> {
    let system: StarSystem = systems.get(system_idx).into();
    if system.is_neutron {
        jump_distance *= Float::from(4.0);
    }
    let mut retval = Vec::new();
    retval.reserve(256);
    for neighbor_idx in (system_idx+1)..systems.len() {
        let neighbor: StarSystem = systems.get(neighbor_idx).into();
        if neighbor.distance_from_sol > system.distance_from_sol + jump_distance {
            break;
        }
        if distance(&system, &neighbor) <= jump_distance {
            retval.push(neighbor_idx);
        }
    }
    if system_idx > 0 {
        for neighbor_idx in (0..(system_idx-1)).rev() {
            let neighbor: StarSystem = systems.get(neighbor_idx).into();
            if neighbor.distance_from_sol < system.distance_from_sol - jump_distance {
                break;
            }
            if distance(&system, &neighbor) <= jump_distance {
                retval.push(neighbor_idx);
            }
        }
    }
    return retval;
}

#[derive(Debug,Copy,Clone)]
struct HScore {
    pub jumps: i64,
    pub distance: Float,
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


pub fn a_star(systems: &IndexedFileMap<StarSystemRecord>, start_idx: u32, goal_idx: u32, jump_distance: Float) -> Option<Vec<u32>> {
    let start: StarSystem = systems.get(start_idx).into();
    let goal: StarSystem = systems.get(goal_idx).into();
    let h_fn = |system_idx: u32| -> HScore {
        let system: StarSystem = systems.get(system_idx).into();
        if system_idx == goal_idx {
            let jumps = 0;
            let distance: Float = 0.0.into();
            HScore{jumps, distance}
        } else if system.is_neutron {
            let distance = distance(&system, &goal);
            let after_neutron_distance: Float = distance - (jump_distance * Float::from(4.0));
            if after_neutron_distance <= 0.0.into() {
                return HScore{jumps: 1, distance};
            }
            let jumps = f32::from((after_neutron_distance / jump_distance).ceil()) as i64 + 1;
            HScore{jumps, distance}
        } else {
            let distance = distance(&system, &goal);
            let jumps = f32::from((distance / jump_distance).ceil()) as i64;
            HScore{jumps, distance}
        }
    };
    let mut parent: HashMap<u32, u32> = HashMap::new();
    let mut g_score = HashMap::new();
    let mut h_score = HashMap::new();
    let mut to_visit = BinaryHeap::new();
    
    let start_h_score = HScore{jumps:0, distance:0.0.into()};
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
        let current: StarSystem = systems.get(current_idx).into();
        println!("[A] Processing {} {:?}\n {:?} {:?}", current.name, current.coords, cur_g, current_h_score);
        for neighbor_idx in neighbors(systems, current_idx, jump_distance) {
            let new_g = HScore{
                jumps: cur_g.jumps+1,
                distance: cur_g.distance + distance(&systems.get(current_idx).into(), &systems.get(neighbor_idx).into()),
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

pub fn neutron_a_star(systems: &IndexedFileMap<StarSystemRecord>, neutron_systems: &IndexedFileMap<NeutronStarSystem>, start_idx: u32, goal_idx: u32, jump_distance: Float) -> Option<Vec<u32>> {
    let start: StarSystem = systems.get(start_idx).into();
    let goal: StarSystem = systems.get(goal_idx).into();

    let total_distance = distance(&start, &goal);

    println!("Neutron star count: {}", neutron_systems.len());

    // let n_to_n_distance: HashMap<(usize, usize), Float> = neutron_systems.iter().enumerate().flat_map(|(i, &start)| {
        // (&neutron_systems[i+1..]).iter().map(move |&end| {
            // ((start, end), distance(&systems[start], &systems[end]))
        // })
    // }).collect();

    let h_fn = |from_idx: u32, system_n_idx: u32| -> HScore {
        let system_idx = neutron_systems.get(system_n_idx).idx;
        let system: StarSystem = systems.get(system_idx).into();
        let from: StarSystem = systems.get(from_idx).into();
        let from_distance = distance(&from, &system);
        let from_jumps = if from.is_neutron {
            let after_first_jump_distance: Float = from_distance - jump_distance * Float::from(4.0);
            if after_first_jump_distance < 0.0.into() {
                1
            } else {
                f32::from((after_first_jump_distance / (jump_distance)).ceil()) as i64 + 1
            }
        } else {
            f32::from((from_distance / jump_distance).ceil()) as i64
        };
        let goal_distance = distance(&system, &goal) / 4.0;
        let goal_jumps = f32::from((goal_distance / jump_distance).ceil()) as i64;
        let jumps = from_jumps + goal_jumps;
        let distance = from_distance + (goal_distance * Float::from(4.0));
        HScore{jumps, distance}
    };

    let RESERVE_SIZE: usize = 500_000;


    let mut g_score = HashMap::new();
    g_score.reserve(RESERVE_SIZE);
    let mut h_score = HashMap::new();
    h_score.reserve(RESERVE_SIZE);
    let mut parent: HashMap<u32, u32> = HashMap::new();
    parent.reserve(RESERVE_SIZE);
    let mut to_visit = BinaryHeap::new();
    to_visit.reserve(RESERVE_SIZE);

    let start_h_score = HScore{jumps: 0, distance: 0.0.into()};
    g_score.insert(start_idx, start_h_score);
    h_score.insert(start_idx, start_h_score);
    
    let no_neutron_path = a_star(systems, start_idx, goal_idx, jump_distance)?;
    let no_neutron_len = no_neutron_path.len() as i64;
    println!("No neutron length: {}", no_neutron_len);
    let no_neutron_h_score = HScore{jumps: no_neutron_len, distance: 0.0.into()};
    h_score.insert(goal_idx, no_neutron_h_score);
    parent.insert(goal_idx, start_idx);

    to_visit.push(Reverse((no_neutron_h_score, goal_idx, 0)));
    for n_idx_idx in 0..neutron_systems.len() {
        let n_system = neutron_systems.get(n_idx_idx);
        let n_idx = n_system.idx;
        let h = h_fn(start_idx, n_idx_idx);
        if h < no_neutron_h_score {
            *h_score.entry(n_idx).or_insert(h) = h;
            to_visit.push(Reverse((h, n_idx, n_idx_idx)));
            *parent.entry(n_idx).or_insert(start_idx) = start_idx;
        }
    }

    let mut num_processed: usize = 0;

    while let Some(Reverse((current_h_score, current_idx, current_idx_idx))) = to_visit.pop() {
        if current_idx == goal_idx {
            break;
        }
        if current_h_score > h_score[&current_idx] {
            continue;
        }


        let parent_idx = parent[&current_idx];
        let parent_s: StarSystem = systems.get(parent_idx).into();
        let current: StarSystem = systems.get(current_idx).into();
        let from_path_len = if parent_s.is_neutron && distance(&parent_s, &current) <= jump_distance * 4.0 {
            1
        } else if distance(&parent_s, &current) <= jump_distance {
            1
        } else {
            let from_path = a_star(systems, parent_idx, current_idx, jump_distance);
            if from_path.is_none() {
                continue;
            }
            let from_path = from_path.unwrap();
            from_path.len() as i64
        };
        let from_path_distance = distance(&parent_s, &current);
        let parent_g_score = g_score[&parent_idx];
        let cur_g_score = HScore{jumps: parent_g_score.jumps + from_path_len, distance:parent_g_score.distance + from_path_distance};
        println!("[N] Queue length: {}", to_visit.len());
        println!("[N] Processing {} {:?}\n {:?} {:?}", current.name, current.coords, cur_g_score, current_h_score);
        *g_score.entry(current_idx).or_insert(cur_g_score) = cur_g_score;
        let to_goal_distance = distance(&current, &goal);
        let to_goal_jumps = f32::from((to_goal_distance / jump_distance).ceil()) as i64;
        let to_goal_h_score = HScore {
            distance: cur_g_score.distance + to_goal_distance,
            jumps: cur_g_score.jumps + to_goal_jumps,
        };
        if to_goal_h_score < h_score[&goal_idx] {
            *h_score.get_mut(&goal_idx).unwrap() = to_goal_h_score;
            to_visit.push(Reverse((to_goal_h_score, goal_idx, 0)));
            *parent.get_mut(&goal_idx).unwrap() = current_idx;
        }

        num_processed += 1;
        if num_processed % 100_000 == 0 {
            println!("Total processed {} ({}%)", num_processed, (num_processed as f64 * 100.0) / (neutron_systems.len() as f64));
        }

        let current_n_system = neutron_systems.get(current_idx_idx);
        for &n_idx_idx in &current_n_system.neighbors {
            let neighbor_idx = neutron_systems.get(n_idx_idx).idx;
            let mut new_h_score = h_fn(current_idx, n_idx_idx);
            new_h_score.jumps += cur_g_score.jumps;
            new_h_score.distance += cur_g_score.distance;
            if new_h_score >= no_neutron_h_score {
                continue;
            }
            if !h_score.contains_key(&neighbor_idx) || new_h_score < h_score[&neighbor_idx] {
                *h_score.entry(neighbor_idx).or_insert(new_h_score) = new_h_score;
                to_visit.push(Reverse((new_h_score, neighbor_idx, n_idx_idx)));
                *parent.entry(neighbor_idx).or_insert(current_idx) = current_idx;
            }
        }
    }

    // println!("Path found, building...");
    let mut path_idx = goal_idx;
    let mut path: Vec<u32> = Vec::new();
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

pub fn make_neutron_star_systems(systems: &Vec<StarSystem>, max_jump_distance: f32) -> Vec<NeutronStarSystem> {
    let max_jump_distance: Float = Float::from(max_jump_distance);
    let neutron_stars: Vec<u32> = systems.iter().enumerate().filter_map(|(i, s)| {
        if s.is_neutron {
            Some(i as u32)
        } else {
            None
        }
    }).collect();
    let total_to_process = neutron_stars.len();
    let mut retval = Arc::new(Mutex::new(Vec::new()));
    retval.lock().unwrap().resize(neutron_stars.len(), NeutronStarSystem{idx: 0, neighbors: Box::new([])});
    let MAX_DISTANCE = Float::from(5000.0);
    //let MAX_DISTANCE_WARN = Float::from(10000.0);
    let n_copied: Vec<(u32, u32)> = neutron_stars.iter().copied().enumerate().map(|(t1, t2)| (t1 as u32, t2)).collect();
    let num_processed = Arc::new(Mutex::new(0));
    n_copied.into_par_iter().for_each(|(start_n_idx, start_idx)| {
        let start = &systems[start_idx as usize];
        let mut sorted: Vec<(u32, u32)> = neutron_stars.iter().copied().enumerate().map(|(t1, t2)| (t1 as u32, t2)).filter(|&(_, i)| distance(start, &systems[i as usize]) < MAX_DISTANCE).collect();
        sorted.sort_by(|&(_,i), &(_,j)| {
            distance(start, &systems[i as usize]).partial_cmp(&distance(start, &systems[j as usize])).unwrap()
        });
        let mut neighbors: Vec<u32> = Vec::new();
        for &(n_idx_idx, n_idx) in &sorted {
            if n_idx == start_idx {
                continue;
            }
            let neigh = &systems[n_idx as usize];
            let d = distance(start, neigh);
            if d <= max_jump_distance {
                neighbors.push(n_idx_idx);
            } else {
                let mut should_add = true;
                for &existing_idx_idx in &neighbors {
                    let existing = &systems[neutron_stars[existing_idx_idx as usize] as usize];
                    if d > distance(neigh, existing) {
                        should_add = false;
                        break;
                    }
                }
                if should_add {
                    neighbors.push(n_idx_idx);
                }
            }
        }
        let neighbors: Box<[u32]> = neighbors.into_boxed_slice();
        {
            let retval = &mut retval.lock().unwrap();
            retval[start_n_idx as usize] = NeutronStarSystem{
                idx: start_idx,
                neighbors
            };
        }
        {
            let mut num_processed = num_processed.lock().unwrap();
            *num_processed += 1;
            if *num_processed % 1_000 == 0 {
                println!("Processed {} {}%", num_processed, (*num_processed as f64 * 100.0) / total_to_process as f64);
            }
        }
    });
    Arc::into_inner(retval).unwrap().into_inner().unwrap()
}

pub fn write_indexed_file<T: bincode::Encode>(data: &[T], output_filepath: &str) -> std::io::Result<()> {
    let bincode_config = bincode::config::standard().with_fixed_int_encoding();
    let out_f = File::create(output_filepath)?;
    let mut buf_out = std::io::BufWriter::new(out_f);
    let mut offset_table: Vec<(usize, u32)> = vec![(0usize, 0u32); data.len()];
    let mut offset_table_size: usize = 0;
    let mut current_offset = bincode::encode_into_std_write(&offset_table_size, &mut buf_out, bincode_config).unwrap();
    if current_offset != 8 {
        panic!("current_offset is {}", current_offset);
    }
    offset_table_size = bincode::encode_into_std_write(&offset_table, &mut buf_out, bincode_config).unwrap();
    current_offset += offset_table_size;
    for (i, datum) in data.iter().enumerate() {
        offset_table[i].0 = current_offset;
        let this_size = bincode::encode_into_std_write(datum, &mut buf_out, bincode_config).unwrap();
        offset_table[i].1 = this_size.try_into().unwrap();
        current_offset += this_size;
    }
    buf_out.seek(std::io::SeekFrom::Start(0)).unwrap();
    bincode::encode_into_std_write(offset_table_size, &mut buf_out, bincode_config).unwrap();
    bincode::encode_into_std_write(offset_table, &mut buf_out, bincode_config).unwrap();
    Ok(())
}

pub struct IndexedFileMap<T: bincode::Decode<()>> {
    file: File,
    map: Mmap,
    offset_table: Vec<(usize, u32)>,
    phantom: std::marker::PhantomData<T>,
}

impl<T: bincode::Decode<()>> IndexedFileMap<T> {
    pub fn new(filepath: &str) -> Self {
        let bincode_config = bincode::config::standard().with_fixed_int_encoding();
        let file = File::open(filepath).unwrap();
        let map = unsafe { Mmap::map(&file).unwrap() };
        map.advise(memmap2::Advice::Sequential).unwrap();
        let (offset_table_size, _): (usize, _) = bincode::decode_from_slice(&map[..8], bincode_config).unwrap();
        let (offset_table, _) = bincode::decode_from_slice(&map[8..offset_table_size+8], bincode_config).unwrap();
        let phantom = std::marker::PhantomData;
        Self {
            file,
            map,
            offset_table,
            phantom,
        }
    }

    pub fn get(&self, index: u32) -> T {
        let bincode_config = bincode::config::standard().with_fixed_int_encoding();
        let (offset, size) = self.offset_table[index as usize];
        let end_offset = offset + (size as usize);
        //println!("Fetching {}: {}..{}", index, offset, end_offset);
        let (retval, _) = bincode::decode_from_slice(&self.map[offset..end_offset], bincode_config).unwrap();
        retval
    }

    pub fn len(&self) -> u32 {
        self.offset_table.len() as u32
    }
}
