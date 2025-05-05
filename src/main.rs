use actix_crud_api::endpoints::{create_payment, create_processing_state, create_record, delete_record, load_the_db, read_payment_details, read_permit_with_filter, read_processing_state, read_record, read_record_by_uuid, read_records_by_opened_date, update_payment_details, update_processing_status, update_records};
use actix_crud_api::struct_definitions::*;
use actix_crud_api::db_setup::setup_db;
use actix_web::{App, HttpServer, web};
use heed::EnvOpenOptions;
use std::sync::Arc;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    let db_path = std::path::Path::new("database");
    std::fs::create_dir_all(db_path)?;

    let env = match unsafe {
        EnvOpenOptions::new()
            .map_size(1024 * 1024 * 1024)
            .max_dbs(1000)
            .open("database")
    } {
        Ok(env) => Arc::new(env),
        Err(e) => {
            println!("Failed to load the env: {}", e);
            std::process::exit(1);
        }
    };

    println!("Environment Opened Successfully");

    let db_handles = setup_db(env.clone())
        .map_err(|e| {
            actix_web::error::ErrorInternalServerError(format!("Error In Setting Up DataBase: {e}"))
        })
        .unwrap();

    let db_handles = web::Data::new(DBdata {
        db_data: Arc::new(db_handles),
    });

    let db_state = web::Data::new(DbEnv { env });
    let url = std::env::var("URL").expect("URL must be set");
    let url = url.trim();

    HttpServer::new(move || {
        App::new()
            .app_data(db_state.clone())
            .app_data(db_handles.clone())
            .service(create_record)
            .service(read_record_by_uuid)
            .service(update_records)
            .service(delete_record)
            .service(read_record)
            .service(load_the_db)
            .service(create_processing_state)
            .service(read_processing_state)
            .service(update_processing_status)
            .service(create_payment)
            .service(update_payment_details)
            .service(read_payment_details)
            .service(read_records_by_opened_date)
            .service(read_permit_with_filter)
    })
    .bind(url)?
    .run()
    .await
}