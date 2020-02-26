use std::{any::Any, collections::HashMap, hash::Hash, marker::PhantomData};
#[derive(Clone, Debug)]
pub struct Key<K, V>(K, PhantomData<V>);


pub struct UniversalMap<K>(HashMap<K, Box<dyn Any>>);


impl<K, V> Key<K, V> {
    pub fn new(key: K) -> Self {
        Self(key, PhantomData)
    }
}

impl<K: Hash + Eq + PartialEq, V> From<K> for Key<K, V> {
    fn from(key: K) -> Self {
        Self::new(key)
    }
}

impl<K: Hash + Eq> Default for UniversalMap<K> {
    fn default() -> Self {
        UniversalMap::new() }
}

impl<K: Eq + Hash> UniversalMap<K> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<V: 'static>(&mut self, key: Key<K, V>, value: V) -> Option<Box<dyn Any>> {
        let result = self.0.insert(key.0, Box::new(value));
        result
    }

    pub fn get<V: 'static>(&self, key: &Key<K, V>) -> Option<&V> {
        match self.0.get(&key.0) {
            Some(value) => value.downcast_ref::<V>(),
            None => None,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

}

#[cfg(test)]
pub mod tests {
    use super::*;

    #[test]
    fn univ_map_can_get_entries() {
        let mut univ_map = UniversalMap::default();

        let key: Key<_, u8> = Key::new("abc");
        let key2: Key<_, bool> = Key::new("def");
        univ_map.insert(key.clone(), 123);
        univ_map.insert(key2.clone(), true);
        assert_eq!(univ_map.get_ref(&key), Some(&123));
        assert_eq!(univ_map.get_ref(&key2), Some(&true))
    }


}
