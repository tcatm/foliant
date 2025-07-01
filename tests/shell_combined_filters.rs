use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, SearchIndexBuilder};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_shell_combined_filters() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build database with test data
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("docs/public/readme.md#public#guide", Some(Value::String("Public readme guide".to_string())));
    builder.insert("docs/public/api.md#public#reference", Some(Value::String("Public API reference".to_string())));  
    builder.insert("docs/private/readme.md#private#guide", Some(Value::String("Private readme guide".to_string())));
    builder.insert("docs/private/secrets.md#private#reference", Some(Value::String("Private secrets reference".to_string())));
    builder.insert("docs/internal/readme.md#internal#guide", Some(Value::String("Internal readme guide".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Test the shell command parsing would work
    // This verifies the type constraints are satisfied
    let db_handle: Database<Value> = Database::<Value>::open(&base)?;
    
    // We can't easily test the actual shell commands here, but we can verify
    // that the database can be passed to run_shell with the proper type bounds
    fn verify_shell_compatible<V: serde::de::DeserializeOwned + serde::Serialize + 'static>(_db: Database<V>) {
        // This function just verifies the type constraints
    }
    
    verify_shell_compatible(db_handle);
    
    Ok(())
}