use std::{collections::HashSet, sync::Arc};
use heed::{types::*, Database, Env};
use crate::struct_definitions::{DBHandles, DBSchema, KeySchema, Payments, ProcessingStatusSchema};

pub fn setup_db(env: Arc<Env>) -> Result<DBHandles, Box<dyn std::error::Error>> {
    let db_path = std::path::Path::new("database");

    if !db_path.exists() {
        std::fs::create_dir_all(db_path)?;
    }

    {
        let mut wtxn = env.write_txn()?;
        
        if env
            .open_database::<Str, SerdeBincode<DBSchema>>(&wtxn, Some("main_db"))?
            .is_none()
        {
            println!("Creating main_db...");
            env.create_database::<Str, SerdeBincode<DBSchema>>(&mut wtxn, Some("main_db"))?;
        }

        if env
            .open_database::<SerdeBincode<KeySchema>, SerdeBincode<HashSet<String>>>(&wtxn, Some("composite_index"))?
            .is_none()
        {
            println!("Creating county_index...");
            env.create_database::<SerdeBincode<KeySchema>, SerdeBincode<HashSet<String>>>(&mut wtxn, Some("composite_index"))?;
        }

        if env
            .open_database::<Str, SerdeBincode<ProcessingStatusSchema>>(&wtxn, Some("processing_state_db"))?
            .is_none()
        {
            println!("Creating Processing Status db...");
            env.create_database::<Str, SerdeBincode<ProcessingStatusSchema>>(&mut wtxn, Some("processing_state_db"))?;
        }

        if env
            .open_database::<Str, SerdeBincode<Payments>>(&wtxn, Some("payments_db"))?
            .is_none()
        {
            println!("Creating Payments db...");
            env.create_database::<Str, SerdeBincode<Payments>>(&mut wtxn, Some("payments_db"))?;
        }
        
        wtxn.commit()?
    }

    let rtxn = env.read_txn()?;

    let main_db = env
        .open_database(&rtxn, Some("main_db"))?
        .unwrap();

    let composite_index = env
        .open_database(&rtxn, Some("composite_index"))?
        .unwrap();

    let processing_state = env
        .open_database(&rtxn, Some("processing_state_db"))?
        .unwrap();

    let payments_db: Database<Str, SerdeBincode<Payments>> = env
        .open_database(&rtxn, Some("payments_db"))?
        .unwrap();

    drop(rtxn);
        
    Ok(DBHandles {
        main_db,
        composite_index,
        processing_state,
        payments_db
    })
}