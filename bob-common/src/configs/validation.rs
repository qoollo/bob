pub trait Validatable {
    fn validate(&self) -> Result<(), String>;
}


pub struct Validator;

impl Validator {
    pub fn aggregate(elements: &[impl Validatable]) -> Result<(), String> {
        let options = elements
            .iter()
            .map(|elem| elem.validate())
            .filter_map(Result::err)
            .collect::<Vec<String>>();
        if options.is_empty() {
            Ok(())
        } else {
            Err(options.join("\n"))
        }
    }

    pub fn validate_no_duplicates<TItem: Eq + std::hash::Hash + Copy>(iter: impl Iterator<Item = TItem>) -> Result<(), TItem> {
        let mut uniq = std::collections::HashSet::new();
        for item in iter {
            if !uniq.insert(item) {
                return Err(item);
            }
        }
        return Ok(());
    }
}