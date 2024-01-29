use anyhow::{anyhow, Result as AnyResult};
use itertools::Itertools;
use bob_common::configs::cluster::Node;
use bob_common::core_types::{DiskName, DiskPath};

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

pub fn pattern_to_nodes(pattern: String, node_pattern: String) -> AnyResult<Vec<Node>> {
    let mut err = Ok(());
    let nodes = generate_range_samples(&pattern)
        .map_while(|key| {
            let result = parse_address_pattern(&key);
            match result {
                Ok(((ip, port), path)) => Some(((ip, port), path)),
                Err(e) => { err = Err(e); None },
            }
        })
        .group_by(|key| key.0.clone())
        .into_iter()
        .enumerate()
        .map(|(node_count, (ip_port, addresses))| {
            let disks: Vec<DiskPath> = addresses
                .enumerate()
                .map(|(disk_count, disk)| {
                    DiskPath::new(
                        DiskName::new(format!("disk{}", disk_count + 1).as_str()),
                        disk.1.as_str(),
                    )
                })
                .collect();

            Node::new(
                substitute_node(node_pattern.as_str(), ip_port.0.as_str(), ip_port.1, node_count + 1),
                format!("{}:{}", ip_port.0, ip_port.1),
                disks,
            )
        })
        .collect();
    err?;
    Ok(nodes)
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
}
