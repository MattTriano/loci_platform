//! IndexedGraph: a routing-ready graph with derived indexes.
//!
//! Built once at Lambda cold start (or once per CLI invocation) from a
//! `Graph` produced by `format::read_from_path`. Holds:
//!
//!   - The underlying `Graph` (CSR adjacency, nodes, edges, geometry).
//!   - A KD-tree over node coordinates for nearest-node lookup.
//!   - The set of nodes that are intersections (out-degree >= 3),
//!     stored as a bit-vector for O(1) membership checks during A*.
//!
//! All derived data is computed eagerly so there's no interior
//! mutability or once-cell synchronization to manage.

use crate::format::Graph;
use crate::kdtree::KdTree;

/// Wraps a Graph with KD-tree and intersection-set indexes.
pub struct IndexedGraph {
    pub graph: Graph,
    /// KD-tree over node coordinates as (lat, lon).
    kdtree: KdTree,
    /// One bit per node, set if the node has degree >= 3.
    /// Matches the Python `get_intersection_nodes` definition.
    intersections: Vec<u64>,
}

impl IndexedGraph {
    /// Build derived indexes for a loaded graph.
    pub fn build(graph: Graph) -> Self {
        let coords: Vec<[f64; 2]> = graph
            .nodes
            .iter()
            .map(|n| [n.lat as f64, n.lon as f64])
            .collect();
        let kdtree = KdTree::build(&coords);
        let intersections = compute_intersection_bitset(&graph);
        Self {
            graph,
            kdtree,
            intersections,
        }
    }

    /// NodeIdx of the nearest node to the given (lat, lon).
    ///
    /// Returns 0 for an empty graph — callers should check
    /// `graph.nodes.is_empty()` first if that case is reachable.
    pub fn nearest_node(&self, lat: f32, lon: f32) -> u32 {
        let q = [lat as f64, lon as f64];
        self.kdtree.nearest(q).unwrap_or(0)
    }

    /// True if the node has degree >= 3 (i.e. is an intersection).
    pub fn is_intersection(&self, node_idx: u32) -> bool {
        let i = node_idx as usize;
        let word = i / 64;
        let bit = i % 64;
        (self.intersections[word] >> bit) & 1 == 1
    }
}

fn compute_intersection_bitset(graph: &Graph) -> Vec<u64> {
    // Python uses NetworkX's `G.degree(n) >= 3`, which counts both
    // in- and out-edges. Our graph is directed and stored with both
    // forward and backward edges for bidirectional segments, so the
    // out-degree alone matches Python's degree for the bidirectional
    // case. For one-way segments the directed in+out degree is what
    // we want; we approximate it by computing in-degree from a
    // single sweep and adding it to out-degree.
    let n = graph.nodes.len();
    let mut total_degree = vec![0u32; n];

    for source_idx in 0..n {
        let out_edges = graph.edges_from(source_idx as u32);
        total_degree[source_idx] += out_edges.len() as u32;
        for e in out_edges {
            total_degree[e.target_node_idx as usize] += 1;
        }
    }

    let word_count = n.div_ceil(64);
    let mut bits = vec![0u64; word_count];
    for (i, &deg) in total_degree.iter().enumerate() {
        if deg >= 3 {
            bits[i / 64] |= 1 << (i % 64);
        }
    }
    bits
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Edge, Graph, Node};

    fn empty_graph() -> Graph {
        Graph {
            heuristic_floor: 0.0,
            strings: vec![],
            nodes: vec![],
            edges: vec![],
            csr_offsets: vec![0],
            segment_geometries: vec![],
        }
    }

    fn nan_edge(target: u32) -> Edge {
        Edge {
            target_node_idx: target,
            segment_id_str_idx: 0,
            name_str_idx: u32::MAX,
            highway_str_idx: u32::MAX,
            infra_type_str_idx: u32::MAX,
            flags: 0b1,
            length_m: 1.0,
            stress_cost: 1.0,
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

    #[test]
    fn three_node_chain_no_intersections() {
        // 0 → 1 → 2 (each node has degree 2 at most)
        let mut g = empty_graph();
        g.strings = vec!["seg".into()];
        g.nodes = vec![
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
            Node {
                osm_id: 3,
                lon: 2.0,
                lat: 0.0,
            },
        ];
        g.edges = vec![nan_edge(1), nan_edge(2)];
        g.csr_offsets = vec![0, 1, 2, 2];

        let idx = IndexedGraph::build(g);
        assert!(!idx.is_intersection(0));
        assert!(!idx.is_intersection(1));
        assert!(!idx.is_intersection(2));
    }

    #[test]
    fn three_way_junction_is_intersection() {
        // 0 → 1, 1 → 2, 1 → 3 (node 1 has out-degree 2, in-degree 1, total 3)
        let mut g = empty_graph();
        g.strings = vec!["seg".into()];
        g.nodes = vec![
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
            Node {
                osm_id: 3,
                lon: 2.0,
                lat: 0.0,
            },
            Node {
                osm_id: 4,
                lon: 1.0,
                lat: 1.0,
            },
        ];
        g.edges = vec![nan_edge(1), nan_edge(2), nan_edge(3)];
        g.csr_offsets = vec![0, 1, 3, 3, 3];

        let idx = IndexedGraph::build(g);
        assert!(!idx.is_intersection(0));
        assert!(idx.is_intersection(1));
        assert!(!idx.is_intersection(2));
        assert!(!idx.is_intersection(3));
    }

    #[test]
    fn nearest_node_basic() {
        let mut g = empty_graph();
        g.nodes = vec![
            Node {
                osm_id: 10,
                lon: -87.6298,
                lat: 41.8781,
            },
            Node {
                osm_id: 20,
                lon: -87.6500,
                lat: 41.8800,
            },
            Node {
                osm_id: 30,
                lon: -87.6100,
                lat: 41.8700,
            },
        ];
        g.csr_offsets = vec![0, 0, 0, 0];

        let idx = IndexedGraph::build(g);
        // Query close to node 0
        let nn = idx.nearest_node(41.8780, -87.6299);
        assert_eq!(nn, 0);
    }
}
