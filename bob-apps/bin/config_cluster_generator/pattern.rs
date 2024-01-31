use anyhow::{anyhow, Result as AnyResult};
use bob_common::configs::cluster::Node;
use bob_common::core_types::{DiskName, DiskPath};
use itertools::Itertools;

fn parse_address_pattern(pattern: &String) -> AnyResult<((String, u16), String)> {
    debug!("Pattern to parse: {}", pattern);
    let re: regex::Regex = regex::Regex::new(r"^([\w.]+):(\d+)(/[\w/]+)$").unwrap();

    if let Some(captures) = re.captures(pattern) {
        let ip = captures.get(1).unwrap().as_str().to_owned();
        let port = captures.get(2).unwrap().as_str();
        let path = captures.get(3).unwrap().as_str().to_owned();

        let port: u16 = port
            .parse()
            .map_err(|_| anyhow!("Failed to parse port {}", port))?;
        Ok(((ip, port), path))
    } else {
        Err(anyhow!("Failed to parse the pattern {}", pattern))
    }
}

fn substitute_node(node_pattern: &str, ip: &str, port: u16, id: usize) -> String {
    node_pattern
        .replace("{ip}", &ip)
        .replace("{port}", &port.to_string())
        .replace("{id}", &id.to_string())
}

fn generate_range_samples(pattern: &str) -> impl Iterator<Item = String> {
    let re = regex::Regex::new(r"\[(\d+)-(\d+)]").unwrap();

    let ranges = re.captures_iter(pattern).map(|captures| {
        let start: usize = captures[1].parse().unwrap();
        let end: usize = captures[2].parse().unwrap();
        start..=end
    });

    re.split(pattern)
        .zip_longest(ranges)
        .map(|x| {
            if let itertools::EitherOrBoth::Both(part, range) = x {
                range.map(|i| part.to_string() + &i.to_string()).collect()
            } else {
                vec![x.left().unwrap().to_string()]
            }
        })
        .multi_cartesian_product()
        .map(|x| x.concat())
        .into_iter()
}

pub fn pattern_extend_nodes(
    mut old_nodes: Vec<Node>,
    pattern: String,
    node_pattern: String,
) -> AnyResult<Vec<Node>> {
    let mut err = Ok(());
    let mut node_counter = old_nodes.len();
    let new_nodes: Vec<Node> = generate_range_samples(&pattern)
        .map_while(|key| {
            let result = parse_address_pattern(&key);
            match result {
                Ok(((ip, port), path)) => Some(((ip, port), path)),
                Err(e) => {
                    err = Err(e);
                    None
                }
            }
        })
        .group_by(|key| key.0.clone())
        .into_iter()
        .filter_map(|(ip_port, addresses)| {
            let address = format!("{}:{}", ip_port.0, ip_port.1);
            let existed_node = old_nodes
                .iter()
                .find_position(|node| node.address() == address);

            let mut old_disks = existed_node
                .map(|(_, node)| node.disks().to_vec())
                .unwrap_or_default();

            let mut disk_counter = old_disks.len();
            let new_disks: Vec<DiskPath> = addresses
                .filter_map(|(_, path)| {
                    let disk_path = path.as_str();
                    if !old_nodes.is_empty() && old_disks.iter().any(|d| d.path() == disk_path) {
                        None
                    } else {
                        disk_counter += 1;
                        Some(DiskPath::new(
                            DiskName::new(&format!("disk{}", disk_counter)),
                            disk_path,
                        ))
                    }
                })
                .collect();
            old_disks.extend_from_slice(&new_disks);
            if let Some((pos, node)) = existed_node {
                old_nodes[pos] = Node::new(node.name().to_string(), address, old_disks);
                None
            } else {
                node_counter += 1;
                Some(Node::new(
                    substitute_node(
                        node_pattern.as_str(),
                        ip_port.0.as_str(),
                        ip_port.1,
                        node_counter,
                    ),
                    address,
                    old_disks,
                ))
            }
        })
        .collect();
    err?;
    old_nodes.extend_from_slice(&new_nodes);
    Ok(old_nodes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_range_samples() {
        let pattern = "abc[1-3]def";
        let samples: Vec<String> = generate_range_samples(pattern).collect();
        assert_eq!(samples, vec!["abc1def", "abc2def", "abc3def"]);

        let pattern = "[0-1]a[1-2]b[2-3]";
        let samples: Vec<String> = generate_range_samples(pattern).collect();
        assert_eq!(
            samples,
            vec!["0a1b2", "0a1b3", "0a2b2", "0a2b3", "1a1b2", "1a1b3", "1a2b2", "1a2b3"]
        );

        let pattern = "a[5-6]b[2-3]c";
        let samples: Vec<String> = generate_range_samples(pattern).collect();
        assert_eq!(samples, vec!["a5b2c", "a5b3c", "a6b2c", "a6b3c"]);

        let pattern = "[5-5]a[0-0]";
        let samples: Vec<String> = generate_range_samples(pattern).collect();
        assert_eq!(samples, vec!["5a0"]);
    }

    #[test]
    fn test_parse_address_pattern() {
        let pattern = String::from("127.0.0.1:8080/disk/path");
        let result = parse_address_pattern(&pattern);
        assert!(result.is_ok());

        let ((ip, port), path) = result.unwrap();
        assert_eq!(ip, "127.0.0.1");
        assert_eq!(port, 8080);
        assert_eq!(path, "/disk/path");

        let pattern = String::from("127.0.0.1:65536/disk/path");
        let result = parse_address_pattern(&pattern);
        assert!(result.is_err());

        let pattern = String::from("a,a:8080/disk/path");
        let result = parse_address_pattern(&pattern);
        assert!(result.is_err());
    }
    #[test]
    fn test_pattern_extend_nodes() {
        let old_nodes = vec![];
        let pattern = "test[1-3]:10000/a[1-2]".to_string();
        let node_pattern = "{ip}_{port}_{id}".to_string();

        let result = pattern_extend_nodes(old_nodes, pattern, node_pattern).unwrap();

        assert_eq!(
            result,
            vec![
                Node::new(
                    "test1_10000_1".to_string(),
                    "test1:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                    ],
                ),
                Node::new(
                    "test2_10000_2".to_string(),
                    "test2:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                    ],
                ),
                Node::new(
                    "test3_10000_3".to_string(),
                    "test3:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                    ],
                ),
            ]
        );
        // extending
        let old_nodes = result;
        let pattern = "test[2-4]:10000/a[2-5]".to_string();
        let node_pattern = "{ip}_{port}_{id}".to_string();

        let result = pattern_extend_nodes(old_nodes, pattern, node_pattern).unwrap();

        assert_eq!(
            result,
            vec![
                Node::new(
                    "test1_10000_1".to_string(),
                    "test1:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                    ],
                ),
                Node::new(
                    "test2_10000_2".to_string(),
                    "test2:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                        DiskPath::new(DiskName::new("disk3"), "/a3"),
                        DiskPath::new(DiskName::new("disk4"), "/a4"),
                        DiskPath::new(DiskName::new("disk5"), "/a5"),
                    ],
                ),
                Node::new(
                    "test3_10000_3".to_string(),
                    "test3:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a1"),
                        DiskPath::new(DiskName::new("disk2"), "/a2"),
                        DiskPath::new(DiskName::new("disk3"), "/a3"),
                        DiskPath::new(DiskName::new("disk4"), "/a4"),
                        DiskPath::new(DiskName::new("disk5"), "/a5"),
                    ],
                ),
                Node::new(
                    "test4_10000_4".to_string(),
                    "test4:10000".to_string(),
                    vec![
                        DiskPath::new(DiskName::new("disk1"), "/a2"),
                        DiskPath::new(DiskName::new("disk2"), "/a3"),
                        DiskPath::new(DiskName::new("disk3"), "/a4"),
                        DiskPath::new(DiskName::new("disk4"), "/a5"),
                    ],
                ),
            ]
        );
    }

    #[test]
    fn test_pattern_extend_nodes_invalid() {
        let old_nodes = vec![];
        let pattern = "test[1-4]:[65535-65537]/a[2-5]".to_string(); // port type: u8
        let node_pattern = "{ip}_{port}_{id}".to_string();
        let result = pattern_extend_nodes(old_nodes, pattern, node_pattern);
        assert!(result.is_err());
    }
}
