use foliant::payload_store::PAYLOAD_STORE_VERSION_V3;
use foliant::{Database, DatabaseBuilder};
use serde_cbor::Value;
use std::error::Error;
use tempfile::tempdir;

#[test]
fn debug_fst_structure_and_bounds() -> Result<(), Box<dyn Error>> {
    let td = tempdir()?;
    let idx = td.path().join("shard.idx");
    let mut builder = DatabaseBuilder::<Value>::new(&idx, PAYLOAD_STORE_VERSION_V3)?;
    
    let keys = &[
        "a/1", "a/2/x", "a/2/y", "a/2/z",
        "b/1", "b/2",
    ];
    
    println!("=== Building FST with keys: {:?} ===", keys);
    for (i, key) in keys.iter().enumerate() {
        builder.insert(key, None);
        println!("Inserted: '{}' at position {}", key, i);
    }
    builder.close()?;

    let mut db = Database::<Value>::new();
    db.add_shard(&idx)?;

    println!("\n=== Full FST Structure Analysis ===");
    
    // Get the shard to access FST internals (we'll need to examine the raw FST)
    if let Some(shard) = db.shards().first() {
        // First, let's see what LUT IDs each key gets
        println!("\n--- Key to LUT ID mapping ---");
        let fst = shard.get_fst();
        for key in keys {
            if let Some(weight) = fst.get(key) {
                println!("Key '{}' -> LUT ID: {}", key, weight);
            }
        }
        
        // Now let's examine the FST transitions at each level
        println!("\n--- FST Transition Analysis ---");
        
        // Use the internal FST to walk transitions
        let raw_fst = fst.as_fst();
        
        // Start from root and examine all outgoing transitions
        println!("Root node transitions:");
        let root = raw_fst.root();
        for i in 0..root.len() {
            let tr = root.transition(i);
            let out = tr.out;
            println!("  Input: '{}' ({}), Output: {}, Addr: {}", 
                tr.inp as char, tr.inp, out.value(), tr.addr);
            
            // Follow this transition and see what's at the target node
            let child_node = raw_fst.node(tr.addr);
            let full_output = out.cat(child_node.final_output());
            println!("    -> Target node final_output: {}, full_output: {}", 
                child_node.final_output().value(), full_output.value());
            
            if child_node.is_final() {
                println!("    -> This is a FINAL node (complete key)");
            }
            
            // Show child transitions
            if child_node.len() > 0 {
                println!("    -> Child transitions:");
                for j in 0..child_node.len() {
                    let child_tr = child_node.transition(j);
                    let child_out = full_output.cat(child_tr.out);
                    let grandchild_node = raw_fst.node(child_tr.addr);
                    let final_child_out = child_out.cat(grandchild_node.final_output());
                    
                    println!("      Input: '{}' ({}), Output: {}, Final: {}", 
                        child_tr.inp as char, child_tr.inp, 
                        child_out.value(), final_child_out.value());
                    
                    if grandchild_node.is_final() {
                        println!("        -> FINAL node");
                    }
                }
            }
        }
        
        // Now let's test our bounds calculation logic manually
        println!("\n--- Manual Bounds Calculation Test ---");
        println!("For transition to 'a/' from root:");
        
        // Find the 'a' transition from root
        for i in 0..root.len() {
            let tr = root.transition(i);
            if tr.inp == b'/' {
                // This would be the transition that leads to common prefixes
                let child_node = raw_fst.node(tr.addr);
                let full_output = tr.out.cat(child_node.final_output());
                
                println!("  Transition output: {}", tr.out.value());
                println!("  Child final_output: {}", child_node.final_output().value());
                println!("  Full LB: {}", full_output.value());
                
                // This should represent the range for the "a/" prefix
                // The question is: what should the UB be?
                
                // Let's check what the next sibling would be by looking for 'b' transitions
                let mut found_next = false;
                for j in i+1..root.len() {
                    let next_tr = root.transition(j);
                    println!("  Next transition: '{}' ({}), output: {}", 
                        next_tr.inp as char, next_tr.inp, next_tr.out.value());
                    
                    if next_tr.inp == b'/' {
                        // This could be the 'b/' transition
                        let next_child = raw_fst.node(next_tr.addr);
                        let next_full = next_tr.out.cat(next_child.final_output());
                        println!("  Next sibling full LB: {}", next_full.value());
                        println!("  So UB for 'a/' should be: {}", next_full.value() - 1);
                        found_next = true;
                        break;
                    }
                }
                
                if !found_next {
                    println!("  No next sibling found, UB should be FST length: {}", raw_fst.len());
                }
                break;
            }
        }
    }
    
    Ok(())
}