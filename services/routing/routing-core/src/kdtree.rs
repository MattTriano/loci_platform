//! /loci_platform/services/routing/routing-core/src/kdtree.rs
//! Static 2D KD-tree for nearest-node lookup.
//!
//! Built once from a slice of points; queried with `nearest`. Operates
//! on Cartesian (lat, lon) without any earth-curvature corrections —
//! the distances are squared planar, which is fine for "find the
//! closest node within a few hundred meters" at the latitudes we
//! care about. If two routing endpoints were very far apart in lat,
//! planar nearest-neighbor could pick a slightly suboptimal node, but
//! the routing endpoint is always near *some* node so the chosen node
//! is the closest one regardless of metric.
//!
//! The implementation is a classic median-split tree stored in a flat
//! `Vec<Node>` with children referenced by index. Build is O(n log n);
//! nearest is O(log n) on average, O(n) worst case for pathological
//! point distributions.

#[derive(Debug, Clone, Copy)]
struct KdNode {
    point: [f64; 2],
    /// Index into the original points slice — what `nearest` returns.
    payload: u32,
    /// Axis split: 0 for x, 1 for y.
    axis: u8,
    /// Index into the `nodes` arena, or `u32::MAX` for "no child".
    left: u32,
    right: u32,
}

const NIL: u32 = u32::MAX;

#[derive(Debug)]
pub struct KdTree {
    nodes: Vec<KdNode>,
    root: u32,
}

impl KdTree {
    /// Build a KD-tree over the given 2D points. Empty input yields an
    /// empty tree which always returns `None` from `nearest`.
    pub fn build(points: &[[f64; 2]]) -> Self {
        // Build with (payload_index, point) so we can sort and still
        // recover the original index.
        let mut items: Vec<(u32, [f64; 2])> = points
            .iter()
            .enumerate()
            .map(|(i, &p)| (i as u32, p))
            .collect();
        let mut nodes = Vec::with_capacity(items.len());
        let root = if items.is_empty() {
            NIL
        } else {
            build_recursive(&mut items, &mut nodes, 0)
        };
        KdTree { nodes, root }
    }

    /// Find the index of the nearest point to `query`. Returns `None`
    /// if the tree is empty.
    pub fn nearest(&self, query: [f64; 2]) -> Option<u32> {
        if self.root == NIL {
            return None;
        }
        let mut best = Best {
            payload: NIL,
            dist_sq: f64::INFINITY,
        };
        nearest_recursive(&self.nodes, self.root, query, &mut best);
        if best.payload == NIL {
            None
        } else {
            Some(best.payload)
        }
    }
}

struct Best {
    payload: u32,
    dist_sq: f64,
}

fn build_recursive(items: &mut [(u32, [f64; 2])], nodes: &mut Vec<KdNode>, depth: u8) -> u32 {
    if items.is_empty() {
        return NIL;
    }
    let axis = depth % 2;
    let mid = items.len() / 2;
    // select_nth_unstable_by is O(n), faster than sorting the whole slice.
    items.select_nth_unstable_by(mid, |a, b| {
        a.1[axis as usize].total_cmp(&b.1[axis as usize])
    });
    let (left_items, rest) = items.split_at_mut(mid);
    let ((payload, point), right_items) = rest.split_first_mut().expect("non-empty");

    let idx = nodes.len() as u32;
    nodes.push(KdNode {
        point: *point,
        payload: *payload,
        axis,
        left: NIL,
        right: NIL,
    });

    let left = build_recursive(left_items, nodes, depth + 1);
    let right = build_recursive(right_items, nodes, depth + 1);
    nodes[idx as usize].left = left;
    nodes[idx as usize].right = right;
    idx
}

fn nearest_recursive(nodes: &[KdNode], idx: u32, query: [f64; 2], best: &mut Best) {
    if idx == NIL {
        return;
    }
    let node = nodes[idx as usize];
    let d_sq = sq_dist(node.point, query);
    if d_sq < best.dist_sq {
        best.dist_sq = d_sq;
        best.payload = node.payload;
    }
    let axis = node.axis as usize;
    let diff = query[axis] - node.point[axis];
    let (near, far) = if diff < 0.0 {
        (node.left, node.right)
    } else {
        (node.right, node.left)
    };
    nearest_recursive(nodes, near, query, best);
    // Visit the far branch only if the splitting plane is within the
    // current best radius.
    if diff * diff < best.dist_sq {
        nearest_recursive(nodes, far, query, best);
    }
}

fn sq_dist(a: [f64; 2], b: [f64; 2]) -> f64 {
    let dx = a[0] - b[0];
    let dy = a[1] - b[1];
    dx * dx + dy * dy
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_tree_returns_none() {
        let t = KdTree::build(&[]);
        assert!(t.nearest([0.0, 0.0]).is_none());
    }

    #[test]
    fn single_point() {
        let t = KdTree::build(&[[1.0, 2.0]]);
        assert_eq!(t.nearest([0.0, 0.0]), Some(0));
    }

    #[test]
    fn three_points_close_query() {
        let points = [[0.0, 0.0], [1.0, 1.0], [2.0, 2.0]];
        let t = KdTree::build(&points);
        assert_eq!(t.nearest([0.1, 0.1]), Some(0));
        assert_eq!(t.nearest([1.4, 1.4]), Some(1));
        assert_eq!(t.nearest([10.0, 10.0]), Some(2));
    }

    #[test]
    fn matches_brute_force_on_random_grid() {
        // Build a grid of points and verify NN against linear scan
        // for a handful of queries.
        let mut points = Vec::new();
        for x in -5..=5 {
            for y in -5..=5 {
                points.push([x as f64, y as f64]);
            }
        }
        let tree = KdTree::build(&points);

        let queries = [
            [0.3, 0.3],
            [-2.7, 1.1],
            [4.9, -4.9],
            [100.0, 100.0],
            [0.0, 0.0],
        ];
        for q in queries {
            let (brute_idx, _) = points
                .iter()
                .enumerate()
                .map(|(i, &p)| (i as u32, sq_dist(p, q)))
                .min_by(|a, b| a.1.total_cmp(&b.1))
                .unwrap();
            let tree_idx = tree.nearest(q).unwrap();
            assert_eq!(
                tree_idx, brute_idx,
                "kd-tree disagreed with brute force at query {q:?}"
            );
        }
    }
}
