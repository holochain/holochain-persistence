use glob::glob;
use holochain_json_api::{
    error::{JsonError, JsonResult},
    json::JsonString,
};
use holochain_persistence_api::{
    cas::content::AddressableContent,
    eav::{
        AddEavi, Attribute, EavFilter, EaviQuery, Entity, EntityAttributeValueIndex, FetchEavi,
        Value,
    },
    error::{PersistenceError, PersistenceResult},
    reporting::ReportStorage,
};
use std::{
    collections::BTreeSet,
    convert::{TryFrom, TryInto},
    fs::{self, create_dir_all, File},
    io::prelude::*,
    marker::{PhantomData, Send, Sync},
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};
use uuid::Uuid;

const ENTITY_DIR: &str = "e";
const ATTRIBUTE_DIR: &str = "a";
const VALUE_DIR: &str = "v";

#[derive(Clone, Debug)]
pub struct EavFileStorage<A: Attribute> {
    dir_path: PathBuf,
    id: Uuid,
    lock: Arc<RwLock<()>>,
    attribute: PhantomData<A>,
}

impl<A: Attribute> PartialEq for EavFileStorage<A> {
    fn eq(&self, other: &EavFileStorage<A>) -> bool {
        self.id == other.id
    }
}

#[warn(unused_must_use)]
pub fn read_eav(parent_path: PathBuf) -> JsonResult<Vec<String>> {
    //glob all  files
    let full_path = parent_path.join("*").join("*.txt");

    let paths = glob(full_path.to_str().unwrap())
        .map_err(|_| JsonError::ErrorGeneric("Could not get form path".to_string()))?;

    // let path_result = paths.last().ok_or(JsonError::ErrorGeneric("Could not get form path".to_string()))?;
    let (eav, error): (BTreeSet<_>, BTreeSet<_>) = paths
        .filter_map(Result::ok)
        .map(|path| {
            fs::read_to_string(&path)
                .map_err(|_| JsonError::ErrorGeneric("Could not read from string".to_string()))
        })
        .partition(Result::is_ok);

    if !error.is_empty() {
        Err(JsonError::ErrorGeneric(
            "Could not read from string".to_string(),
        ))
    } else {
        Ok(eav.iter().cloned().filter_map(Result::ok).collect())
    }
}

impl<A: Attribute> EavFileStorage<A>
where
    A: std::string::ToString + serde::de::DeserializeOwned,
{
    pub fn new<P: AsRef<Path>>(dir_path: P) -> JsonResult<EavFileStorage<A>> {
        let dir_path = dir_path.as_ref().into();

        Ok(EavFileStorage {
            dir_path,
            id: Uuid::new_v4(),
            lock: Arc::new(RwLock::new(())),
            attribute: PhantomData,
        })
    }

    fn write_to_file(
        &self,
        subscript: String,
        eav: &EntityAttributeValueIndex<A>,
    ) -> JsonResult<()> {
        let address: String = match &*subscript {
            ENTITY_DIR => eav.entity().to_string(),
            ATTRIBUTE_DIR => eav.attribute().to_string(),
            VALUE_DIR => eav.value().to_string(),
            _ => String::new(),
        };

        let path = self
            .dir_path
            .join(&subscript)
            .join(&address)
            .join(&eav.index().to_string());

        create_dir_all(&path)?;

        let address_path = path.join(eav.address().to_string());

        let full_path = address_path.with_extension("txt");

        let mut file = File::create(full_path)?;
        writeln!(file, "{}", eav.content())?;
        Ok(())
    }

    fn read_from_dir<T>(
        &self,
        subscript: String,
        eav_filter: &EavFilter<T>,
    ) -> JsonResult<BTreeSet<String>>
    where
        T: Eq + ToString + TryFrom<String>,
    {
        let path = self.dir_path.join(&subscript);

        if path.exists() {
            let full_path = path.join("*");

            let paths = glob(full_path.to_str().unwrap())
                .map_err(|_| JsonError::ErrorGeneric("Could not get form path".to_string()))?;

            let (paths, errors): (Vec<_>, Vec<_>) = paths.partition(Result::is_ok);
            let eavs = paths
                .into_iter()
                .map(|p| p.unwrap())
                .filter(|pathbuf| {
                    pathbuf
                        .iter()
                        .last()
                        .and_then(|v| {
                            let v = v.to_string_lossy();
                            v.to_string()
                                .try_into()
                                .map_err(|_| println!("warn/eav: invalid EAV string: {}", v))
                                .ok()
                                .map(|val| eav_filter.check(val))
                        })
                        .unwrap_or_default()
                })
                .map(|pathbuf| read_eav(pathbuf));
            if !errors.is_empty() {
                Err(JsonError::ErrorGeneric(
                    "Could not read eavs from directory".to_string(),
                ))
            } else {
                Ok(eavs.filter_map(|s| s.ok()).flatten().collect())
            }
        } else {
            Ok(BTreeSet::new())
        }
    }
}

impl<A: Attribute> AddEavi<A> for EavFileStorage<A>
where
    A: std::string::ToString
        + TryFrom<String>
        + Sync
        + Send
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TryFrom<JsonString>
        + Into<JsonString>,
{
    fn add_eavi(
        &self,
        eav: &EntityAttributeValueIndex<A>,
    ) -> PersistenceResult<Option<EntityAttributeValueIndex<A>>> {
        let _guard = self.lock.write()?;
        let wild_card = Path::new("*");
        //create glob path to query file system parentdir/*/*/*/{address}.txt
        let text_file_path = Path::new(&eav.address().to_string()).with_extension("txt");
        let path: PathBuf = self
            .dir_path
            .join(wild_card)
            .join(ENTITY_DIR)
            .join(&*eav.index().to_string())
            .join(&text_file_path);

        //if next exists create a new eav with a different index
        let eav = if path.exists() {
            EntityAttributeValueIndex::new(&eav.entity(), &eav.attribute(), &eav.value())?
        } else {
            eav.clone()
        };

        self.write_to_file(ENTITY_DIR.to_string(), &eav)
            .and_then(|_| self.write_to_file(ATTRIBUTE_DIR.to_string(), &eav))
            .and_then(|_| self.write_to_file(VALUE_DIR.to_string(), &eav))
            .map(|_| Some(eav.clone()))
            .map_err(|err| err.into())
    }
}

impl<A: Attribute> FetchEavi<A> for EavFileStorage<A>
where
    A: std::string::ToString
        + TryFrom<String>
        + Sync
        + Send
        + serde::Serialize
        + serde::de::DeserializeOwned
        + TryFrom<JsonString>
        + Into<JsonString>,
{
    fn fetch_eavi(
        &self,
        query: &EaviQuery<A>,
    ) -> PersistenceResult<BTreeSet<EntityAttributeValueIndex<A>>> {
        let _guard = self.lock.read()?;

        let entity_set = self.read_from_dir::<Entity>(ENTITY_DIR.to_string(), query.entity())?;
        let attribute_set =
            self.read_from_dir::<A>(ATTRIBUTE_DIR.to_string(), query.attribute())?;
        let value_set = self.read_from_dir::<Value>(VALUE_DIR.to_string(), query.value())?;

        let attribute_value_inter: BTreeSet<String> =
            value_set.intersection(&attribute_set).cloned().collect();
        let entity_attribute_value_inter: BTreeSet<String> = attribute_value_inter
            .intersection(&entity_set)
            .cloned()
            .collect();

        //still a O(n) structure because they are slipt in different places.
        let (eavis, errors): (BTreeSet<_>, BTreeSet<_>) = entity_attribute_value_inter
            .into_iter()
            .map(|content| {
                EntityAttributeValueIndex::try_from_content(&JsonString::from_json(&content))
            })
            .partition(Result::is_ok);
        if !errors.is_empty() {
            // not all EAVs were converted
            Err(PersistenceError::ErrorGeneric(
                "Error Converting EAVs".to_string(),
            ))
        } else {
            let it = eavis.iter().map(|e| {
                e.clone()
                    .expect("no problem here since we have filtered out all bad conversions")
            });
            let results = query.run(it);
            Ok(results)
        }
    }
}

impl<A: Attribute> ReportStorage for EavFileStorage<A> {}

#[cfg(test)]
pub mod tests {
    use crate::eav::file::EavFileStorage;
    use holochain_json_api::json::RawString;
    use holochain_persistence_api::{
        cas::{
            content::{AddressableContent, ExampleAddressableContent},
            storage::EavTestSuite,
        },
        eav::ExampleAttribute,
    };
    use tempfile::tempdir;

    #[test]
    fn file_eav_round_trip() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let entity_content =
            ExampleAddressableContent::try_from_content(&RawString::from("foo").into()).unwrap();
        let attribute = ExampleAttribute::WithPayload("favourite-color".to_string());
        let value_content =
            ExampleAddressableContent::try_from_content(&RawString::from("blue").into()).unwrap();

        EavTestSuite::test_round_trip(
            EavFileStorage::new(temp_path).unwrap(),
            entity_content,
            attribute,
            value_content,
        )
    }

    #[test]
    fn file_eav_one_to_many() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage: EavFileStorage<ExampleAttribute> = EavFileStorage::new(temp_path).unwrap();
        EavTestSuite::test_one_to_many::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn file_eav_many_to_one() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavFileStorage::new(temp_path).unwrap();
        EavTestSuite::test_many_to_one::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn file_eav_range() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavFileStorage::new(temp_path).unwrap();
        EavTestSuite::test_range::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(eav_storage, &ExampleAttribute::default());
    }

    #[test]
    fn file_eav_prefixes() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavFileStorage::new(temp_path).unwrap();
        EavTestSuite::test_multiple_attributes::<
            ExampleAddressableContent,
            ExampleAttribute,
            EavFileStorage<ExampleAttribute>,
        >(
            eav_storage,
            vec!["a_", "b_", "c_", "d_"]
                .into_iter()
                .map(|p| ExampleAttribute::WithPayload(p.to_string() + "one_to_many"))
                .collect(),
        );
    }

    #[test]
    fn file_tombstone() {
        let temp = tempdir().expect("test was supposed to create temp dir");
        let temp_path = String::from(temp.path().to_str().expect("temp dir could not be string"));
        let eav_storage = EavFileStorage::new(temp_path).unwrap();
        EavTestSuite::test_tombstone::<ExampleAddressableContent, EavFileStorage<_>>(eav_storage)
    }
}
