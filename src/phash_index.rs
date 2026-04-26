use crate::features::phash_to_u64;
use anyhow::Result;
use rusqlite::Connection;
use std::collections::{HashMap, HashSet};

#[derive(Debug, Clone, Copy)]
struct IndexedPHash {
    phash_bits: i64,
    phash_value: u64,
}

#[derive(Debug, Default)]
pub struct PhashIndex {
    trees: HashMap<i64, BKTree>,
    entries: HashMap<i64, IndexedPHash>,
}

impl PhashIndex {
    pub fn from_catalog(conn: &Connection) -> Result<Self> {
        let mut index = Self::default();
        let mut stmt = conn.prepare(
            "SELECT id, phash, phash_bits FROM target_items WHERE phash <> '' AND phash_bits > 0",
        )?;
        let rows = stmt.query_map([], |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        for row in rows {
            let (id, phash, phash_bits) = row?;
            index.upsert(id, &phash, phash_bits);
        }
        Ok(index)
    }

    pub fn upsert(&mut self, id: i64, phash: &str, phash_bits: i64) {
        let Some(phash_value) = phash_to_u64(phash) else {
            self.entries.remove(&id);
            return;
        };

        self.entries.insert(
            id,
            IndexedPHash {
                phash_bits,
                phash_value,
            },
        );
        self.trees
            .entry(phash_bits)
            .or_default()
            .add(phash_value, id);
    }

    pub fn search(&self, phash: &str, phash_bits: i64, max_distance: u32) -> Vec<i64> {
        let Some(query) = phash_to_u64(phash) else {
            return Vec::new();
        };
        let Some(tree) = self.trees.get(&phash_bits) else {
            return Vec::new();
        };

        let mut seen = HashSet::new();
        let mut hits = Vec::new();
        for hit in tree.search(query, max_distance) {
            let Some(current) = self.entries.get(&hit.id) else {
                continue;
            };
            if current.phash_bits != phash_bits {
                continue;
            }
            let distance = hamming_distance(query, current.phash_value);
            if distance > max_distance {
                continue;
            }
            if seen.insert(hit.id) {
                hits.push((distance, hit.id));
            }
        }

        hits.sort_by_key(|(distance, id)| (*distance, *id));
        hits.into_iter().map(|(_, id)| id).collect()
    }
}

#[derive(Debug, Clone, Copy)]
struct SearchHit {
    id: i64,
}

#[derive(Debug, Default)]
struct BKTree {
    root: Option<Box<BKNode>>,
}

#[derive(Debug)]
struct BKNode {
    hash: u64,
    ids: Vec<i64>,
    children: HashMap<u32, Box<BKNode>>,
}

impl BKTree {
    fn add(&mut self, hash: u64, id: i64) {
        let Some(root) = self.root.as_mut() else {
            self.root = Some(Box::new(BKNode {
                hash,
                ids: vec![id],
                children: HashMap::new(),
            }));
            return;
        };

        let mut current = root;
        loop {
            let distance = hamming_distance(current.hash, hash);
            if distance == 0 && current.hash == hash {
                if !current.ids.contains(&id) {
                    current.ids.push(id);
                }
                return;
            }

            if !current.children.contains_key(&distance) {
                current.children.insert(
                    distance,
                    Box::new(BKNode {
                        hash,
                        ids: vec![id],
                        children: HashMap::new(),
                    }),
                );
                return;
            }
            current = current
                .children
                .get_mut(&distance)
                .expect("child should exist after contains_key");
        }
    }

    fn search(&self, query: u64, max_distance: u32) -> Vec<SearchHit> {
        let Some(root) = self.root.as_ref() else {
            return Vec::new();
        };

        let mut results = Vec::new();
        let mut stack = vec![root.as_ref()];
        while let Some(node) = stack.pop() {
            let distance = hamming_distance(node.hash, query);
            if distance <= max_distance {
                results.extend(node.ids.iter().copied().map(|id| SearchHit { id }));
            }

            let min_distance = distance.saturating_sub(max_distance);
            let max_distance = distance + max_distance;
            for (edge_distance, child) in &node.children {
                if *edge_distance >= min_distance && *edge_distance <= max_distance {
                    stack.push(child.as_ref());
                }
            }
        }

        results
    }
}

fn hamming_distance(left: u64, right: u64) -> u32 {
    (left ^ right).count_ones()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bk_tree_search_returns_threshold_matches() {
        let mut tree = BKTree::default();
        tree.add(0b1111, 1);
        tree.add(0b1110, 2);
        tree.add(0b0000, 3);

        let hits = tree.search(0b1111, 1);
        let ids: Vec<i64> = hits.into_iter().map(|hit| hit.id).collect();
        assert_eq!(ids, vec![1, 2]);
    }
}
