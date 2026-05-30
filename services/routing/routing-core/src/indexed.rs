//! /loci_platform/services/routing/routing-core/src/indexed.rs
//! IndexedGraph: a routing-ready graph with derived indexes.
//!
//! Built once at Lambda cold start (or once per CLI invocation) from a
//! `Graph` produced by `format::read_from_path`. Holds:
//!
//!   - The underlying `Graph` (CSR adjacency, nodes, edges, geometry).
//!   - A KD-tree over node coordinates for nearest-node lookup.
//!   - A map from segment string-index to its geometry record, so
//!     response composition is O(1) per edge instead of a linear scan.
//!
//! Intersection status is read directly from `Node.is_intersection`
//! (populated by the data pipeline), so there's no degree-counting or
//! cached bitset to keep in sync.

use std::collections::HashMap;

use crate::format::{Graph, SegmentGeometry};
use crate::kdtree::KdTree;

/// Wraps a Graph with KD-tree and segment-geometry indexes.
pub struct IndexedGraph {
    pub graph: Graph,
    /// KD-tree over node coordinates as (lat, lon).
    kdtree: KdTree,
    /// segment_id string index -> position in `graph.segment_geometries`.
    segment_geometry_by_str_idx: HashMap<u32, usize>,
}

impl IndexedGraph {
    /// Build derived indexes for a loaded graph.
    pub fn build(graph: Graph) -> Self {
        let coords: Vec<[f64; 2]> = graph.nodes.iter().map(|n| [n.lat, n.lon]).collect();
        let kdtree = KdTree::build(&coords);

        let segment_geometry_by_str_idx = graph
            .segment_geometries
            .iter()
            .enumerate()
            .map(|(i, sg)| (sg.segment_id_str_idx, i))
            .collect();

        Self {
            graph,
            kdtree,
            segment_geometry_by_str_idx,
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

    /// True if the node is a logical intersection. Read straight from
    /// the node record; the data pipeline owns the definition.
    pub fn is_intersection(&self, node_idx: u32) -> bool {
        self.graph.nodes[node_idx as usize].is_intersection
    }

    /// Geometry for a segment, by its string-table index. O(1).
    pub fn segment_geometry(&self, seg_str_idx: u32) -> Option<&SegmentGeometry> {
        self.segment_geometry_by_str_idx
            .get(&seg_str_idx)
            .map(|&i| &self.graph.segment_geometries[i])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Graph, Node};

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

    fn node(osm_id: u64, lon: f64, lat: f64, is_intersection: bool) -> Node {
        Node {
            osm_id,
            lon,
            lat,
            is_intersection,
        }
    }

    #[test]
    fn reads_intersection_flag_from_nodes() {
        // is_intersection now comes straight off the node record rather
        // than being derived from degree.
        let mut g = empty_graph();
        g.nodes = vec![
            node(1, 0.0, 0.0, false),
            node(2, 1.0, 0.0, true),
            node(3, 2.0, 0.0, false),
        ];
        g.csr_offsets = vec![0, 0, 0, 0];

        let idx = IndexedGraph::build(g);
        assert!(!idx.is_intersection(0));
        assert!(idx.is_intersection(1));
        assert!(!idx.is_intersection(2));
    }

    #[test]
    fn nearest_node_basic() {
        let mut g = empty_graph();
        g.nodes = vec![
            node(10, -87.6298, 41.8781, false),
            node(20, -87.6500, 41.8800, false),
            node(30, -87.6100, 41.8700, false),
        ];
        g.csr_offsets = vec![0, 0, 0, 0];

        let idx = IndexedGraph::build(g);
        // Query close to node 0
        let nn = idx.nearest_node(41.8780, -87.6299);
        assert_eq!(nn, 0);
    }

    #[test]
    fn segment_geometry_lookup_indexes_by_str_idx() {
        use crate::format::SegmentGeometry;

        let mut g = empty_graph();
        g.nodes = vec![node(1, 0.0, 0.0, false)];
        g.csr_offsets = vec![0, 0];
        g.segment_geometries = vec![
            SegmentGeometry {
                segment_id_str_idx: 7,
                coords: vec![(1.0, 2.0)],
            },
            SegmentGeometry {
                segment_id_str_idx: 3,
                coords: vec![(3.0, 4.0)],
            },
        ];

        let idx = IndexedGraph::build(g);
        assert_eq!(idx.segment_geometry(7).unwrap().coords, vec![(1.0, 2.0)]);
        assert_eq!(idx.segment_geometry(3).unwrap().coords, vec![(3.0, 4.0)]);
        assert!(idx.segment_geometry(99).is_none());
    }
}
