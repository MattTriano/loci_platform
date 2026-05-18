//! Turn-aware A* shortest-path.
//!
//! Port of `_astar_with_turn_costs` from routing.py. The state space
//! is `(node, prev_node)` at intersections and `(node, None)`
//! elsewhere, so the left-turn penalty (which depends on incoming
//! direction) can be applied correctly without blowing up the state
//! space at every node.

use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};

use crate::geom::haversine_m;
use crate::indexed::IndexedGraph;
use crate::turn_cost::compute_left_turn_penalty;

/// State key: (current node, predecessor node).
/// `predecessor` is None for the start node and for any non-intersection
/// node where turn context doesn't apply.
type State = (u32, Option<u32>);

/// Priority-queue entry. `Reverse` ordering on `(f_score, counter)` gives
/// min-heap behavior with stable tie-breaking, matching the Python heapq
/// implementation.
#[derive(Debug)]
struct HeapEntry {
    f_score: f32,
    counter: u64,
    state: State,
}

impl PartialEq for HeapEntry {
    fn eq(&self, other: &Self) -> bool {
        self.f_score == other.f_score && self.counter == other.counter
    }
}
impl Eq for HeapEntry {}
impl PartialOrd for HeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse order on f_score (min-heap), then on counter (stable).
        // NaN f_scores are treated as worse than any real value so they
        // sink to the bottom of the queue — they shouldn't occur in
        // practice (heuristic is non-negative, costs are non-negative)
        // but the total_cmp guards against UB if they ever do.
        other
            .f_score
            .total_cmp(&self.f_score)
            .then_with(|| other.counter.cmp(&self.counter))
    }
}

/// Run turn-aware A* from `source` to `target` (both as NodeIdx).
///
/// Returns the ordered list of node indices from source to target,
/// inclusive. Returns `None` if no path exists.
pub fn astar_with_turn_costs(g: &IndexedGraph, source: u32, target: u32) -> Option<Vec<u32>> {
    if source == target {
        return Some(vec![source]);
    }

    let heuristic_floor = g.graph.heuristic_floor as f32;
    let heuristic = |from: u32, to: u32| -> f32 {
        let f = g.graph.nodes[from as usize];
        let t = g.graph.nodes[to as usize];
        heuristic_floor * haversine_m(f.lat, f.lon, t.lat, t.lon)
    };

    let mut open: BinaryHeap<HeapEntry> = BinaryHeap::new();
    let mut g_score: HashMap<State, f32> = HashMap::new();
    let mut came_from: HashMap<State, State> = HashMap::new();
    let mut closed: HashMap<State, ()> = HashMap::new();
    let mut counter: u64 = 0;

    let start_state: State = (source, None);
    g_score.insert(start_state, 0.0);
    open.push(HeapEntry {
        f_score: heuristic(source, target),
        counter,
        state: start_state,
    });

    while let Some(HeapEntry { state, .. }) = open.pop() {
        let (curr, prev) = state;

        if curr == target {
            return Some(reconstruct_path(&came_from, state));
        }

        if closed.contains_key(&state) {
            continue;
        }
        closed.insert(state, ());

        let curr_g = g_score[&state];

        for edge in g.graph.edges_from(curr) {
            let next = edge.target_node_idx;
            let edge_cost = edge.stress_cost;

            let turn_penalty = match prev {
                Some(prev_node) => compute_left_turn_penalty(
                    &g.graph,
                    prev_node,
                    curr,
                    next,
                    edge,
                    g.is_intersection(curr),
                ),
                None => 0.0,
            };

            let tentative_g = curr_g + edge_cost + turn_penalty;

            // The predecessor we record for `next` depends only on
            // whether `next` is an intersection. Non-intersection
            // nodes collapse to a single state regardless of how we
            // got there, keeping the search space tight.
            let next_prev = if g.is_intersection(next) {
                Some(curr)
            } else {
                None
            };
            let next_state: State = (next, next_prev);

            if closed.contains_key(&next_state) {
                continue;
            }

            let existing = g_score.get(&next_state).copied().unwrap_or(f32::INFINITY);
            if tentative_g < existing {
                g_score.insert(next_state, tentative_g);
                came_from.insert(next_state, state);
                counter += 1;
                let f_score = tentative_g + heuristic(next, target);
                open.push(HeapEntry {
                    f_score,
                    counter,
                    state: next_state,
                });
            }
        }
    }

    None
}

fn reconstruct_path(came_from: &HashMap<State, State>, end: State) -> Vec<u32> {
    let mut path = vec![end.0];
    let mut cur = end;
    while let Some(&prev) = came_from.get(&cur) {
        path.push(prev.0);
        cur = prev;
    }
    path.reverse();
    path
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Edge, Graph, Node};
    use crate::indexed::IndexedGraph;

    fn edge(target: u32, length: f32, stress: f32, highway_str_idx: u32) -> Edge {
        Edge {
            target_node_idx: target,
            segment_id_str_idx: 0,
            name_str_idx: u32::MAX,
            highway_str_idx,
            infra_type_str_idx: u32::MAX,
            flags: 0b1,
            length_m: length,
            stress_cost: stress,
            speed_factor: f32::NAN,
            road_type_factor: f32::NAN,
            infrastructure_factor: f32::NAN,
            tunnel_factor: f32::NAN,
            surface_factor: f32::NAN,
            lighting_factor: f32::NAN,
            physical_cost: f32::NAN,
            intersection_cost: f32::NAN,
            crash_cost: f32::NAN,
        }
    }

    fn make_graph(nodes: Vec<Node>, strings: Vec<String>, edge_groups: Vec<Vec<Edge>>) -> Graph {
        let mut edges = Vec::new();
        let mut csr_offsets = vec![0u32];
        for group in edge_groups {
            edges.extend(group);
            csr_offsets.push(edges.len() as u32);
        }
        Graph {
            heuristic_floor: 0.0,
            strings,
            nodes,
            edges,
            csr_offsets,
            segment_geometries: vec![],
        }
    }

    #[test]
    fn straight_three_node_path() {
        // 0 → 1 → 2, each edge length 100, stress 100. Only one path.
        let g = make_graph(
            vec![
                Node {
                    osm_id: 1,
                    lon: 0.0,
                    lat: 0.0,
                },
                Node {
                    osm_id: 2,
                    lon: 0.0,
                    lat: 0.001,
                },
                Node {
                    osm_id: 3,
                    lon: 0.0,
                    lat: 0.002,
                },
            ],
            vec!["seg".into()],
            vec![
                vec![edge(1, 100.0, 100.0, u32::MAX)],
                vec![edge(2, 100.0, 100.0, u32::MAX)],
                vec![],
            ],
        );
        let idx = IndexedGraph::build(g);
        let path = astar_with_turn_costs(&idx, 0, 2).unwrap();
        assert_eq!(path, vec![0, 1, 2]);
    }

    #[test]
    fn source_equals_target_is_single_node() {
        let g = make_graph(
            vec![Node {
                osm_id: 1,
                lon: 0.0,
                lat: 0.0,
            }],
            vec![],
            vec![vec![]],
        );
        let idx = IndexedGraph::build(g);
        let path = astar_with_turn_costs(&idx, 0, 0).unwrap();
        assert_eq!(path, vec![0]);
    }

    #[test]
    fn no_path_returns_none() {
        // Two disconnected nodes
        let g = make_graph(
            vec![
                Node {
                    osm_id: 1,
                    lon: 0.0,
                    lat: 0.0,
                },
                Node {
                    osm_id: 2,
                    lon: 1.0,
                    lat: 0.0,
                },
            ],
            vec![],
            vec![vec![], vec![]],
        );
        let idx = IndexedGraph::build(g);
        assert!(astar_with_turn_costs(&idx, 0, 1).is_none());
    }

    #[test]
    fn prefers_lower_stress_path() {
        // Two routes from 0 to 3:
        //   direct: 0 → 3 (length 200, stress 500)
        //   detour: 0 → 1 → 2 → 3 (each leg length 100, stress 100, total 300)
        // The detour has lower total stress and should be chosen.
        let g = make_graph(
            vec![
                Node {
                    osm_id: 1,
                    lon: 0.0,
                    lat: 0.0,
                },
                Node {
                    osm_id: 2,
                    lon: 0.001,
                    lat: 0.0,
                },
                Node {
                    osm_id: 3,
                    lon: 0.001,
                    lat: 0.001,
                },
                Node {
                    osm_id: 4,
                    lon: 0.0,
                    lat: 0.002,
                },
            ],
            vec!["seg".into()],
            vec![
                // 0 → 1, 0 → 3 (direct)
                vec![
                    edge(1, 100.0, 100.0, u32::MAX),
                    edge(3, 200.0, 500.0, u32::MAX),
                ],
                // 1 → 2
                vec![edge(2, 100.0, 100.0, u32::MAX)],
                // 2 → 3
                vec![edge(3, 100.0, 100.0, u32::MAX)],
                vec![],
            ],
        );
        let idx = IndexedGraph::build(g);
        let path = astar_with_turn_costs(&idx, 0, 3).unwrap();
        assert_eq!(path, vec![0, 1, 2, 3]);
    }

    #[test]
    fn left_turn_penalty_steers_routing() {
        // Setup: cyclist arrives at intersection N1 from the south (N0)
        // and can reach the goal N4 via either:
        //   left turn:  0 → 1 → 2 → 4  (target edge "primary", penalty 30 × 2.5 = 75)
        //   right turn: 0 → 1 → 3 → 4  (target edge "primary", no penalty)
        //
        // All edge costs are equal (100 each). Without the penalty
        // both routes tie; with the penalty the right-turn route wins
        // by 75 virtual cost.
        //
        // Coordinates (lat, lon):
        //   N0 (0.0, 0.0)         south of N1
        //   N1 (0.001, 0.0)       intersection
        //   N2 (0.001, -0.001)    west of N1  (left turn from N0)
        //   N3 (0.001, 0.001)     east of N1  (right turn from N0)
        //   N4 (0.002, 0.0)       far north, reachable from both
        //
        // Node 1 needs degree >= 3 to qualify as an intersection. It
        // gets 1 in-edge (from 0) and 2 out-edges (to 2, to 3) → degree 3.
        let g = make_graph(
            vec![
                Node {
                    osm_id: 1,
                    lon: 0.0,
                    lat: 0.0,
                },
                Node {
                    osm_id: 2,
                    lon: 0.0,
                    lat: 0.001,
                },
                Node {
                    osm_id: 3,
                    lon: -0.001,
                    lat: 0.001,
                },
                Node {
                    osm_id: 4,
                    lon: 0.001,
                    lat: 0.001,
                },
                Node {
                    osm_id: 5,
                    lon: 0.0,
                    lat: 0.002,
                },
            ],
            vec!["seg".into(), "primary".into()],
            vec![
                // 0 → 1
                vec![edge(1, 100.0, 100.0, u32::MAX)],
                // 1 → 2 (left, primary), 1 → 3 (right, primary)
                vec![edge(2, 100.0, 100.0, 1), edge(3, 100.0, 100.0, 1)],
                // 2 → 4
                vec![edge(4, 100.0, 100.0, u32::MAX)],
                // 3 → 4
                vec![edge(4, 100.0, 100.0, u32::MAX)],
                vec![],
            ],
        );
        let idx = IndexedGraph::build(g);
        assert!(idx.is_intersection(1), "node 1 must be an intersection");

        let path = astar_with_turn_costs(&idx, 0, 4).unwrap();
        // Right-turn path: 0 → 1 → 3 → 4 (node 3 is at lon -0.001, the
        // "right turn" target from a northward arrival at N1).
        assert_eq!(
            path,
            vec![0, 1, 3, 4],
            "expected right-turn route to be preferred"
        );
    }
}
