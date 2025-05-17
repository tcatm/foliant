use index::Entry;

/// Collect all entries from the iterator, sort them, and return a Vec<Entry>.
pub fn collect_sorted<I>(iter: I) -> Vec<Entry>
where
    I: IntoIterator<Item = Entry>,
{
    let mut v: Vec<_> = iter.into_iter().collect();
    v.sort();
    v
}