use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder, Entry, Streamer, SearchIndexBuilder};
use serde_json::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn test_unicode_search_basic() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with Unicode content
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("café", Some(Value::String("Coffee shop".to_string())));
    builder.insert("naïve", Some(Value::String("Simple".to_string())));
    builder.insert("fiancée", Some(Value::String("Engaged woman".to_string())));
    builder.insert("résumé", Some(Value::String("CV".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for "cafe" should find "café"
    let results: Vec<Entry<Value>> = db.search(None, "cafe")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "café");
    
    // Search for accented version
    let results: Vec<Entry<Value>> = db.search(None, "café")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "café");
    
    // Search for partial match
    let results: Vec<Entry<Value>> = db.search(None, "résu")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "résumé");
    
    Ok(())
}

#[test]
fn test_unicode_search_emojis() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with emojis
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("hello👋world", Some(Value::String("Greeting".to_string())));
    builder.insert("🍕pizza", Some(Value::String("Food".to_string())));
    builder.insert("cat🐱lover", Some(Value::String("Animal person".to_string())));
    builder.insert("👨‍👩‍👧‍👦family", Some(Value::String("ZWJ sequence".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for emoji
    let results: Vec<Entry<Value>> = db.search(None, "👋")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "hello👋world");
    
    // Search for text with emoji
    let results: Vec<Entry<Value>> = db.search(None, "🍕piz")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "🍕pizza");
    
    // Search for ZWJ sequence (family emoji)
    let results: Vec<Entry<Value>> = db.search(None, "👨‍👩‍👧‍👦")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "👨‍👩‍👧‍👦family");
    
    Ok(())
}

#[test]
fn test_unicode_search_mixed_scripts() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with mixed scripts
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    builder.insert("hello世界", Some(Value::String("Hello world in Chinese".to_string())));
    builder.insert("Привет мир", Some(Value::String("Hello world in Russian".to_string())));
    builder.insert("مرحبا العالم", Some(Value::String("Hello world in Arabic".to_string())));
    builder.insert("こんにちは世界", Some(Value::String("Hello world in Japanese".to_string())));
    builder.insert("🇺🇸🇯🇵🇩🇪flags", Some(Value::String("Country flags".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for Chinese characters
    let results: Vec<Entry<Value>> = db.search(None, "世界")?.collect();
    assert_eq!(results.len(), 2); // Should find both Chinese and Japanese entries
    
    // Search for Russian (case insensitive)
    let results: Vec<Entry<Value>> = db.search(None, "привет")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "Привет мир");
    
    // Search for flags (regional indicators)
    let results: Vec<Entry<Value>> = db.search(None, "🇺🇸")?.collect();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].as_str(), "🇺🇸🇯🇵🇩🇪flags");
    
    Ok(())
}

#[test]
fn test_unicode_search_normalization() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with different Unicode normalization forms
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    // Ligatures
    builder.insert("ﬁle", Some(Value::String("fi ligature".to_string())));
    builder.insert("ﬂower", Some(Value::String("fl ligature".to_string())));
    // Full-width
    builder.insert("ＨＥＬＬＯ", Some(Value::String("Full-width".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for "file" should find ligature version
    let results: Vec<Entry<Value>> = db.search(None, "file")?.collect();
    assert_eq!(results.len(), 1);
    
    // Search for "flower" should find ligature version
    let results: Vec<Entry<Value>> = db.search(None, "flower")?.collect();
    assert_eq!(results.len(), 1);
    
    // Search for "hello" should find full-width version
    let results: Vec<Entry<Value>> = db.search(None, "hello")?.collect();
    assert_eq!(results.len(), 1);
    
    Ok(())
}

#[test]
fn test_unicode_search_edge_cases() -> Result<(), Box<dyn Error>> {
    let dir = tempdir()?;
    let base = dir.path().join("db.idx");
    
    // Build index with edge cases
    let mut builder = DatabaseBuilder::<Value>::new(&base, PAYLOAD_STORE_VERSION_V3)?;
    // Skin tone modifiers
    builder.insert("wave👋🏽to", Some(Value::String("Wave with skin tone".to_string())));
    // Zero-width joiner sequences
    builder.insert("dev👨‍💻work", Some(Value::String("Developer emoji".to_string())));
    // Combining diacritics (zalgo text)
    builder.insert("ḩ̸̢̻̈ë̸́l̸̰̈l̶̰̽o̶̱̍", Some(Value::String("Zalgo text".to_string())));
    // Right-to-left
    builder.insert("שלום עולם", Some(Value::String("Hebrew hello world".to_string())));
    // Invisible characters
    builder.insert("hello\u{200b}world", Some(Value::String("Zero-width space".to_string())));
    builder.close()?;
    
    // Build search index
    let mut db = Database::<Value>::open(&base)?;
    SearchIndexBuilder::build_index(&mut db, None)?;
    
    // Search for emoji with skin tone
    let results: Vec<Entry<Value>> = db.search(None, "👋🏽")?.collect();
    assert_eq!(results.len(), 1);
    
    // Search for ZWJ sequence
    let results: Vec<Entry<Value>> = db.search(None, "👨‍💻")?.collect();
    assert_eq!(results.len(), 1);
    
    // Search for Hebrew
    let results: Vec<Entry<Value>> = db.search(None, "שלום")?.collect();
    assert_eq!(results.len(), 1);
    
    Ok(())
}