//! /loci_platform/services/routing/routing-core/src/find_route.rs
//! Public routing entry point.
//!
//! Composes the A* result into a structured response matching the
//! existing Lambda's output. Serialization to JSON happens in the
//! Lambda handler and CLI, not here.

use thiserror::Error;

use crate::astar::astar_with_turn_costs;
use crate::format::Edge;
use crate::indexed::IndexedGraph;
use crate::turn_cost::compute_left_turn_penalty;

/// Result of a successful routing query.
///
/// Field names mirror the JSON keys produced by the Python Lambda for
/// straightforward serialization.
#[derive(Debug, Clone)]
pub struct RouteResult {
    pub total_cost: f32,
    pub total_length_m: f32,
    /// OSM IDs of the visited nodes in order.
    pub nodes: Vec<u64>,
    pub segments: Vec<RouteSegment>,
}

#[derive(Debug, Clone)]
pub struct RouteSegment {
    /// (lon, lat) pairs ready for the frontend. f64 to match the graph
    /// format's coordinate precision.
    pub coordinates: Vec<(f64, f64)>,
    pub length_m: f32,
    pub stress_cost: f32,
    pub name: Option<String>,
    pub highway: Option<String>,
    pub infra_type: Option<String>,
    pub cost_components: CostComponents,
}

#[derive(Debug, Clone)]
pub struct CostComponents {
    pub length_m: f32,
    pub physical_cost: f32,
    pub crash_cost: f32,
    pub intersection_cost: f32,
    pub elevation_cost: f32,
    pub left_turn_penalty: f32,
}

#[derive(Error, Debug)]
pub enum FindRouteError {
    #[error("no route found between origin and destination")]
    NoPath,
    #[error("origin or destination node not present in graph")]
    NodeNotFound,
    #[error(
        "internal: directed edge {from}->{to} expected but not present \
         (graph data inconsistent with A* path)"
    )]
    MissingEdge { from: u64, to: u64 },
}

/// Find the best route between two (lat, lon) coordinates.
pub fn find_route(
    g: &IndexedGraph,
    origin_lat: f32,
    origin_lon: f32,
    dest_lat: f32,
    dest_lon: f32,
) -> Result<RouteResult, FindRouteError> {
    let origin_idx = g.nearest_node(origin_lat, origin_lon);
    let dest_idx = g.nearest_node(dest_lat, dest_lon);

    if origin_idx == dest_idx {
        let n = &g.graph.nodes[origin_idx as usize];
        return Ok(RouteResult {
            total_cost: 0.0,
            total_length_m: 0.0,
            nodes: vec![n.osm_id],
            segments: vec![],
        });
    }

    let path = astar_with_turn_costs(g, origin_idx, dest_idx).ok_or(FindRouteError::NoPath)?;
    compose_result(g, &path)
}

/// Walk the A* path, look each edge back up to recover its attributes,
/// and assemble the full response shape.
fn compose_result(g: &IndexedGraph, path: &[u32]) -> Result<RouteResult, FindRouteError> {
    // Per-source edge lookup: for each consecutive (u, v) pair we need
    // the edge u → v. We scan source u's edges_from(u) for the one
    // whose target is v. CSR slices are small (per-node out-degree),
    // so this is cheap.

    let mut segments = Vec::with_capacity(path.len().saturating_sub(1));
    let mut total_cost = 0.0_f32;
    let mut total_length_m = 0.0_f32;
    let mut prev_node: Option<u32> = None;

    for window in path.windows(2) {
        let u = window[0];
        let v = window[1];
        let edge = find_edge(g, u, v).ok_or(FindRouteError::MissingEdge {
            from: g.graph.nodes[u as usize].osm_id,
            to: g.graph.nodes[v as usize].osm_id,
        })?;

        let edge_cost = edge.stress_cost;
        let edge_length = edge.length_m;

        let left_turn_penalty = match prev_node {
            Some(p) => compute_left_turn_penalty(&g.graph, p, u, v, edge, g.is_intersection(u)),
            None => 0.0,
        };

        total_cost += edge_cost + left_turn_penalty;
        total_length_m += edge_length;

        let coords = segment_coords(g, edge, u, v);

        segments.push(RouteSegment {
            coordinates: coords,
            length_m: edge_length,
            stress_cost: edge_cost,
            name: g.graph.lookup_str(edge.name_str_idx).map(str::to_owned),
            highway: g.graph.lookup_str(edge.highway_str_idx).map(str::to_owned),
            infra_type: g
                .graph
                .lookup_str(edge.infra_type_str_idx)
                .map(str::to_owned),
            cost_components: CostComponents {
                length_m: edge_length,
                physical_cost: edge.physical_cost,
                crash_cost: edge.crash_cost,
                intersection_cost: edge.intersection_cost,
                elevation_cost: edge.elevation_cost,
                left_turn_penalty,
            },
        });

        prev_node = Some(u);
    }

    let nodes: Vec<u64> = path
        .iter()
        .map(|&i| g.graph.nodes[i as usize].osm_id)
        .collect();

    Ok(RouteResult {
        total_cost,
        total_length_m,
        nodes,
        segments,
    })
}

/// Locate the directed edge u → v in the CSR adjacency.
fn find_edge<'g>(g: &'g IndexedGraph, u: u32, v: u32) -> Option<&'g Edge> {
    g.graph
        .edges_from(u)
        .iter()
        .find(|e| e.target_node_idx == v)
}

/// Resolve segment geometry for an edge, reversing if the edge is the
/// backward sibling. Falls back to a two-point straight line if no
/// geometry is stored for the segment.
fn segment_coords(g: &IndexedGraph, edge: &Edge, u: u32, v: u32) -> Vec<(f64, f64)> {
    match g.segment_geometry(edge.segment_id_str_idx) {
        Some(sg) => {
            if edge.is_forward() {
                sg.coords.clone()
            } else {
                let mut rev = sg.coords.clone();
                rev.reverse();
                rev
            }
        }
        None => {
            let u_node = g.graph.nodes[u as usize];
            let v_node = g.graph.nodes[v as usize];
            vec![(u_node.lon, u_node.lat), (v_node.lon, v_node.lat)]
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::format::{Edge, Graph, Node, SegmentGeometry};

    fn node(osm_id: u64, lon: f64, lat: f64) -> Node {
        Node {
            osm_id,
            lon,
            lat,
            is_intersection: false,
        }
    }

    fn edge(target: u32, length: f32, stress: f32, seg_str_idx: u32, forward: bool) -> Edge {
        Edge {
            target_node_idx: target,
            segment_id_str_idx: seg_str_idx,
            name_str_idx: u32::MAX,
            highway_str_idx: u32::MAX,
            infra_type_str_idx: u32::MAX,
            flags: if forward { 0b1 } else { 0 },
            length_m: length,
            stress_cost: stress,
            physical_cost: stress,
            intersection_cost: 0.0,
            crash_cost: 0.0,
            elevation_cost: 0.0,
        }
    }

    #[test]
    fn straight_route_assembles_response() {
        let g = Graph {
            heuristic_floor: 0.0,
            strings: vec!["seg_a".into(), "seg_b".into()],
            nodes: vec![
                node(100, 0.0, 0.0),
                node(200, 0.0, 0.001),
                node(300, 0.0, 0.002),
            ],
            edges: vec![
                edge(1, 100.0, 100.0, 0, true),
                edge(2, 100.0, 100.0, 1, true),
            ],
            csr_offsets: vec![0, 1, 2, 2],
            segment_geometries: vec![
                SegmentGeometry {
                    segment_id_str_idx: 0,
                    coords: vec![(0.0, 0.0), (0.0, 0.001)],
                },
                SegmentGeometry {
                    segment_id_str_idx: 1,
                    coords: vec![(0.0, 0.001), (0.0, 0.002)],
                },
            ],
        };
        let idx = IndexedGraph::build(g);
        let result = find_route(&idx, 0.0, 0.0, 0.002, 0.0).unwrap();

        assert_eq!(result.nodes, vec![100, 200, 300]);
        assert_eq!(result.segments.len(), 2);
        assert!((result.total_length_m - 200.0).abs() < 0.01);
        assert!((result.total_cost - 200.0).abs() < 0.01);
        assert_eq!(
            result.segments[0].coordinates,
            vec![(0.0, 0.0), (0.0, 0.001)]
        );
    }

    #[test]
    fn backward_edge_reverses_geometry() {
        let g = Graph {
            heuristic_floor: 0.0,
            strings: vec!["seg".into()],
            nodes: vec![node(100, 0.0, 0.0), node(200, 0.0, 0.001)],
            // Edge 0 → 1 is the backward sibling: geometry stored
            // 1→0, returned reversed.
            edges: vec![edge(1, 100.0, 100.0, 0, false)],
            csr_offsets: vec![0, 1, 1],
            segment_geometries: vec![SegmentGeometry {
                segment_id_str_idx: 0,
                coords: vec![(0.0, 0.001), (0.0, 0.0)],
            }],
        };
        let idx = IndexedGraph::build(g);
        let result = find_route(&idx, 0.0, 0.0, 0.001, 0.0).unwrap();
        // Reversed: coords should run from u=(0,0) to v=(0,0.001).
        assert_eq!(
            result.segments[0].coordinates,
            vec![(0.0, 0.0), (0.0, 0.001)]
        );
    }

    #[test]
    fn missing_geometry_falls_back_to_endpoints() {
        let g = Graph {
            heuristic_floor: 0.0,
            strings: vec!["seg".into()],
            nodes: vec![node(100, 0.0, 0.0), node(200, 0.0, 0.001)],
            edges: vec![edge(1, 100.0, 100.0, 0, true)],
            csr_offsets: vec![0, 1, 1],
            segment_geometries: vec![], // none — fallback triggered
        };
        let idx = IndexedGraph::build(g);
        let result = find_route(&idx, 0.0, 0.0, 0.001, 0.0).unwrap();
        assert_eq!(
            result.segments[0].coordinates,
            vec![(0.0, 0.0), (0.0, 0.001)]
        );
    }

    #[test]
    fn coincident_origin_destination_returns_single_node() {
        let g = Graph {
            heuristic_floor: 0.0,
            strings: vec![],
            nodes: vec![node(100, 0.0, 0.0)],
            edges: vec![],
            csr_offsets: vec![0, 0],
            segment_geometries: vec![],
        };
        let idx = IndexedGraph::build(g);
        let result = find_route(&idx, 0.0, 0.0, 0.0, 0.0).unwrap();
        assert_eq!(result.nodes, vec![100]);
        assert!(result.segments.is_empty());
    }

    #[test]
    fn no_route_returns_error() {
        let g = Graph {
            heuristic_floor: 0.0,
            strings: vec![],
            nodes: vec![node(100, 0.0, 0.0), node(200, 1.0, 0.0)],
            edges: vec![],
            csr_offsets: vec![0, 0, 0],
            segment_geometries: vec![],
        };
        let idx = IndexedGraph::build(g);
        let err = find_route(&idx, 0.0, 0.0, 0.0, 1.0).unwrap_err();
        assert!(matches!(err, FindRouteError::NoPath));
    }
}
