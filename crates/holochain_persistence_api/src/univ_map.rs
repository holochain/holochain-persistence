use std::{any::Any, collections::HashMap, hash::Hash, marker::PhantomData};


#[derive(Clone, Debug)]
pub struct Key<K, V>(K, PhantomData<V>);

#[derive(Clone, Debug)]
pub struct UniversalMap<K:Eq+Hash, VV>(HashMap<K, Box<VV>>);

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

impl<K: Hash + Eq, VV:'static> Default for UniversalMap<K, VV> {
    fn default() -> Self {
        UniversalMap::new()
    }
}

impl<K: Eq + Hash, VV:'static + Any> UniversalMap<K, VV> {
    pub fn new() -> Self {
        Self(HashMap::new())
    }

    pub fn insert<V: 'static + Any>(&mut self, key: Key<K, V>, value: V) ->
        Result<Option<Box<VV>>, String> {

        let value = Box::new(value);
        let value_any = value as Box<dyn Any>;
        let downcast_result = value_any.downcast::<VV>();

        match downcast_result {
            Ok(v) => {
                println!("INSERT!");
                let result = self.0.insert(key.0, v);
                Ok(result)
            }
            Err(_e) => Err("Cannot cast V to VV".into())
        }
   }

    pub fn get<V: 'static>(&self, key: &Key<K, V>) -> Option<&V> {
        self.0.get(&key.0).and_then(|value| {
            let value_any = value.as_ref() as &dyn Any;
            let downcast_result = value_any.downcast_ref::<V>();
            downcast_result
        }
        )
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
        let mut univ_map : UniversalMap<_, Box<dyn std::any::Any>> = UniversalMap::default();

        let key: Key<_, u8> = Key::new("abc");
        let key2: Key<_, bool> = Key::new("def");
        let _result = univ_map.insert(key.clone(), 123);
        let _result = univ_map.insert(key2.clone(), true);
        assert_eq!(univ_map.get(&key), Some(&123));
        assert_eq!(univ_map.get(&key2), Some(&true))
    }
}
