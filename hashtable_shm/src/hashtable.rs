use std::{
    collections::LinkedList,
    hash::{DefaultHasher, Hash, Hasher},
    sync::RwLock,
};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("Number of buckets cannot be zero")]
    BucketSizeZero,

    #[error("key already exists")]
    KeyExists,

    #[error("key is missing")]
    KeyMissing,
}

#[derive(Debug)]
struct Entry<K: Clone + Hash + Eq, V: Clone> {
    key: K,
    val: V,
}

#[derive(Debug)]
/// Simple HashTable with synchronization
///
pub struct HashTable<K: Clone + Hash + Eq, V: Clone> {
    size: usize,
    buckets: Vec<RwLock<LinkedList<Entry<K, V>>>>,
}

impl<K: Clone + Hash + Eq, V: Clone> HashTable<K, V> {
    /// Creates new HashTable
    ///
    /// size must be > 0
    pub fn new(size: usize) -> Result<Self, Error> {
        if size == 0 {
            return Err(Error::BucketSizeZero);
        }
        let mut buckets = Vec::with_capacity(size);
        for _ in 0..size {
            buckets.push(RwLock::new(LinkedList::new()));
        }

        Ok(Self { size, buckets })
    }

    fn hash(&self, key: &K) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);

        hasher.finish() as usize % self.size
    }

    /// Add entry to HashTable
    ///
    /// returns error if key already exists
    pub fn add(&self, key: K, val: V) -> Result<(), Error> {
        let pos = self.hash(&key);

        let bucket: &mut std::sync::RwLockWriteGuard<'_, LinkedList<Entry<K, V>>> =
            &mut self.buckets[pos].write().unwrap();

        if bucket.iter().any(|x| x.key == key) {
            return Err(Error::KeyExists);
        }

        bucket.push_back(Entry { key, val });

        Ok(())
    }

    /// Read entry from HashTable
    pub fn read(&self, key: &K) -> Option<V> {
        let pos = self.hash(key);
        let bucket = &self.buckets[pos].read().unwrap();

        bucket.iter().find(|x| x.key == *key).map(|x| x.val.clone())
    }

    /// Delete entry from HashTable
    ///
    /// returns error when the key does not exists
    pub fn delete(&self, key: &K) -> Result<(), Error> {
        let pos = self.hash(key);
        let bucket = &mut self.buckets[pos].write().unwrap();

        // LinkedList in the rust standard library does not provide an interface to remove an entry if you have a pointer to it.
        // This works around it, but is then not in ideal runtime, as we are searching first for the position and iterating again to remove the item from the list.
        // Alternatives are: implementing own LinkedList, enabling the nightly option for it or using generally recommended vector based containers
        match bucket.iter().position(|x| x.key == *key) {
            Some(entry_pos) => {
                let mut rest_list = bucket.split_off(entry_pos);
                rest_list.pop_front();
                bucket.append(&mut rest_list);
                Ok(())
            }
            None => Err(Error::KeyMissing),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]

    fn zero_bucket_table() {
        let table: Result<HashTable<u32, u32>, Error> = HashTable::new(0);
        assert!(table.is_err());
    }

    #[test]
    fn add_read() {
        let table: HashTable<u32, u32> = HashTable::new(10).expect("could not create table");
        let res = table.add(24, 54);
        assert!(res.is_ok());
        assert_eq!(table.read(&24), Some(54));

        // Try to add to a key that already exists
        let res = table.add(24, 62);
        assert!(res.is_err());
        assert_eq!(table.read(&24), Some(54));
    }

    #[test]
    fn single_bucket() {
        let table: HashTable<u32, u32> = HashTable::new(1).expect("could not create table");

        let res = table.add(1, 4);
        assert!(res.is_ok());

        let res = table.add(2, 5);
        assert!(res.is_ok());

        let res = table.add(3, 6);
        assert!(res.is_ok());

        let res = table.delete(&2);
        assert!(res.is_ok());

        assert_eq!(table.read(&1), Some(4));
        assert_eq!(table.read(&3), Some(6));
        assert_eq!(table.read(&2), None);

        let res = table.delete(&2);
        assert!(res.is_err());
    }
}
