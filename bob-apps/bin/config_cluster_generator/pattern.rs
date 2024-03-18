use anyhow::{anyhow, Result as AnyResult};
use bob_common::configs::cluster::Node;
use bob_common::core_types::{DiskName, DiskPath};
use itertools::Itertools;
use regex::{Captures, Regex};

type Sample = ((String, u16), String);

fn parse_address_sample(sample: &str) -> AnyResult<Sample> {
    debug!("Sample to parse: {}", sample);
    let re = Regex::new(r"^(?<addr>[\w.]+):(?<port>\d+)(?<path>/[\w/]+)$").unwrap();
    if let Some(captures) = re.captures(sample) {
        let ip = captures["addr"].to_owned();
        let port = captures["port"]
            .parse::<u16>()
            .map_err(|_| anyhow!("Failed to parse port {}", &captures["port"]))?;
        let path = captures["path"].to_owned();
        Ok(((ip, port), path))
    } else {
        Err(anyhow!("Failed to parse the sample {}", sample))
    }
}

fn substitute_node_pattern(node_pattern: &str, ip: &str, port: u16, id: usize) -> String {
    let re = Regex::new(r"\{(\w+)}").unwrap();
    re.replace_all(node_pattern, |caps: &Captures| match &caps[1] {
        "ip" => ip.to_string(),
        "port" => port.to_string(),
        "id" => id.to_string(),
        _ => caps[0].to_string(),
    })
    .to_string()
}

fn generate_range_samples(pattern: &str) -> impl Iterator<Item = String> {
    let re = Regex::new(r"\[(\d+)-(\d+)]").unwrap();

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
                vec![x.left().expect("is some because split > range").to_string()]
            }
        })
        .multi_cartesian_product()
        .map(|x| x.concat())
        .into_iter()
}

fn pattern_extend_disks(node: &mut Node, disk_paths: impl Iterator<Item = String>) {
    let old_disks: std::collections::HashSet<_> = node.disks().iter().map(|d| d.path()).collect();
    let new_disks: Vec<DiskPath> = disk_paths
        .filter(|disk_path| !old_disks.contains(disk_path.as_str()))
        .enumerate()
        .map(|(idx, disk_path)| {
            DiskPath::new(
                DiskName::new(&format!("disk{}", idx + old_disks.len() + 1)),
                disk_path.as_str(),
            )
        })
        .collect();
    node.disks_extend(new_disks);
}

fn extend_nodes_by_samples(
    nodes: &mut Vec<Node>,
    parsed_samples: &Vec<Sample>,
    node_pattern: String,
) {
    let existing_addresses: std::collections::HashSet<_> =
        nodes.iter().map(|node| node.address()).collect();
    let mut new_nodes = Vec::new();
    for (ip_port, _) in &parsed_samples.iter().group_by(|(ip_port, _)| ip_port) {
        let address = format!("{}:{}", ip_port.0, ip_port.1);
        if !existing_addresses.contains(address.as_str()) {
            let new_node = Node::new(
                substitute_node_pattern(
                    &node_pattern,
                    &ip_port.0,
                    ip_port.1,
                    nodes.len() + new_nodes.len() + 1,
                ),
                address.to_owned(),
                vec![],
            );
            new_nodes.push(new_node);
        }
    }
    nodes.extend(new_nodes);
}

pub fn pattern_extend_nodes(
    mut nodes: Vec<Node>,
    pattern: String,
    node_pattern: String,
) -> AnyResult<Vec<Node>> {
    let parsed_samples = generate_range_samples(&pattern)
        .map(|key| parse_address_sample(&key))
        .collect::<AnyResult<Vec<_>>>()?;

    extend_nodes_by_samples(&mut nodes, &parsed_samples, node_pattern);
    for (ip_port, paths) in &parsed_samples.iter().group_by(|(ip_port, _)| ip_port) {
        if let Some(node) = nodes
            .iter_mut()
            .find(|node| node.address() == format!("{}:{}", ip_port.0, ip_port.1))
        {
            let disks_to_extend = paths.map(|(_, path)| path.to_owned());
            pattern_extend_disks(node, disks_to_extend);
        }
    }

    Ok(nodes)
}

#[cfg(test)]
mod tests {
    use super::Node;
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
        let result = parse_address_sample(&pattern);
        assert!(result.is_ok());

        let ((ip, port), path) = result.unwrap();
        assert_eq!(ip, "127.0.0.1");
        assert_eq!(port, 8080);
        assert_eq!(path, "/disk/path");

        let pattern = String::from("127.0.0.1:65536/disk/path");
        let result = parse_address_sample(&pattern);
        assert!(result.is_err());

        let pattern = String::from("a,a:8080/disk/path");
        let result = parse_address_sample(&pattern);
        assert!(result.is_err());
    }
    #[test]
    fn test_pattern_extend_nodes() {
        let nodes = vec![];
        let pattern = "test[1-3]:10000/a[1-2]".to_string();
        let node_pattern = "{ip}_{port}_{id}".to_string();

        let result = pattern_extend_nodes(nodes, pattern, node_pattern).unwrap();

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
        let pattern = "test[1-4]:[65535-65537]/a[2-5]".to_string(); // port type: u16
        let node_pattern = "{ip}_{port}_{id}".to_string();
        let result = pattern_extend_nodes(old_nodes, pattern, node_pattern);
        assert!(result.is_err());
    }
}
