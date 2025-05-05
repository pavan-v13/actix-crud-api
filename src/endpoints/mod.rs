use std::{
    collections::{HashMap, HashSet},
    str::FromStr,
};

use actix_web::{HttpResponse, Responder, delete, get, post, put, web};
use chrono::{NaiveDate, NaiveDateTime};
use heed::RoTxn;
use serde_json::json;

use crate::{
    handle,
    helper_functions::{data_with_response_time, data_with_response_time_for_slice, loader},
    struct_definitions::{
        DBSchema, DBdata, DbEnv, KeySchema, Payments, ProcessingStatusSchema, Status,
        UpdateDBSchema, UpdatePayment, UpdateProcessingStatusSchema,
    },
};

#[post("/create-record")]
pub async fn create_record(
    db_env: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    data: web::Json<DBSchema>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let uuid = uuid::Uuid::new_v4().to_string();
    let uuid = format!("{:?}-{}", data.opened, uuid);

    let key = KeySchema {
        client: data.client.to_owned(),
        county: data.county.to_owned(),
        county_status: data.county_status.clone(),
    };

    let uuid_set = handle!(db_handles.db_data.composite_index.get(&wtxn, &key));
    handle!(db_handles.db_data.main_db.put(&mut wtxn, &uuid, &data));

    if let Some(mut prev_set) = uuid_set {
        prev_set.insert(uuid.to_owned());
        handle!(
            db_handles
                .db_data
                .composite_index
                .put(&mut wtxn, &key, &mut prev_set)
        );
    } else {
        let mut new_set: HashSet<String> = HashSet::new();
        new_set.insert(uuid.to_owned());
        handle!(
            db_handles
                .db_data
                .composite_index
                .put(&mut wtxn, &key, &mut new_set)
        );
    }

    let start = std::time::Instant::now();
    handle!(wtxn.commit());
    let duration = start.elapsed();

    Ok(HttpResponse::Ok().body(format!(
        "Successfully Loaded into DataBase, uuid is: {}\nResponse Time: {}",
        uuid,
        duration.as_micros()
    )))
}

#[post("/create-processing-status/{permit_number}")]
pub async fn create_processing_state(
    db_env: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    data: web::Json<ProcessingStatusSchema>,
    path: web::Path<String>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let key = path.into_inner();

    let processing_state_data = ProcessingStatusSchema {
        processing_status: data.processing_status.to_owned(),
        due_date: data.due_date,
        last_modified: data.last_modified,
        assigned_to: data.assigned_to.to_owned(),
    };

    let indexing_key = format!("{key}-{:?}", data.last_modified);

    handle!(db_handles.db_data.processing_state.put(
        &mut wtxn,
        &indexing_key,
        &processing_state_data
    ));

    let start = std::time::Instant::now();
    handle!(wtxn.commit());
    let duration = start.elapsed().as_micros();

    Ok(HttpResponse::Ok().body(format!(
        "Successfully added processing state for permit number: {key}\nResponse Time: {duration}{indexing_key}"
    )))
}

#[post("/create-payment/{permit_numer}")]
pub async fn create_payment(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    path: web::Path<String>,
    data: web::Json<Payments>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let permit_number = path.into_inner();
    let key = format!("{permit_number}-{:?}", data.date);

    let payment_data = Payments {
        date: data.date,
        payment: data.payment.to_owned(),
        amount: data.amount,
        status: data.status.to_owned(),
    };

    handle!(
        db_handles
            .db_data
            .payments_db
            .put(&mut wtxn, &key, &payment_data)
    );

    let start = std::time::Instant::now();
    handle!(wtxn.commit());
    let duration = start.elapsed().as_micros();

    Ok(HttpResponse::Ok().body(format!("Successfully added payment data for permit_numer: {permit_number}\nResponse Time: {duration}")))
}

#[get("/read-payment-details/{permit_number}")]
pub async fn read_payment_details(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    path: web::Path<String>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let rtxn = handle!(db_env.env.read_txn());
    let permit_numbers = path.into_inner();
    let permit_numbers: Vec<&str> = permit_numbers.split(",").collect();
    let mut final_result = vec![];
    let start = std::time::Instant::now();

    for permit_number in permit_numbers {
        let key = format!("{permit_number}-");
        let db = db_handles.db_data.payments_db;
        let mut cursor = db.prefix_iter(&rtxn, &key)?;
        let mut result = vec![];

        while let Some(Ok((key, value))) = cursor.next() {
            result.push((key, value))
        }

        final_result.push(result);
    }

    let duration = start.elapsed().as_micros();
    let response = json!({
        "Response Time": duration,
        "Data": final_result
    });

    Ok(HttpResponse::Ok().json(response))
}

#[get("/read-processing-status/{permit_number}")]
pub async fn read_processing_state(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    path: web::Path<String>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let rtxn = handle!(db_env.env.read_txn());
    let keys = path.into_inner();
    let keys: Vec<&str> = keys.split(",").collect();
    let mut final_result = vec![];

    for key in keys {
        let key = format!("{key}-");
        let db = db_handles.db_data.processing_state;
        let mut cursor = db.prefix_iter(&rtxn, &key)?;
        let mut result = vec![];

        while let Some(Ok((key, value))) = cursor.next() {
            result.push((key, value))
        }

        final_result.push(result);
    }

    let duration = start.elapsed().as_micros();
    let response = json!({
        "Response Time": duration,
        "Data": final_result
    });

    Ok(HttpResponse::Ok().json(response))
}

#[get("/read-record-by-uuid/{key}")]
pub async fn read_record_by_uuid(
    db: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    path: web::Path<String>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    let key = path.into_inner();
    let rtxn = db.env.read_txn().unwrap();

    let main_db = db_handles.db_data.main_db;

    let record = {
        match main_db.get(&rtxn, &key) {
            Ok(Some(record)) => Some(record),
            Ok(_) => {
                return Ok(HttpResponse::NotFound()
                    .body(format!("No Record found with the uuid: {}", key)));
            }
            Err(e) => {
                eprintln!("Database error: {}", e);
                return Ok(HttpResponse::InternalServerError().body("Database Error"));
            }
        }
    };

    if let Some(record) = record {
        let duration = start.elapsed();
        return Ok(HttpResponse::Ok().body(format!(
            "permit_link: {}\npermit_number: {}\nclient: {}\nopened_date: {}\nlast_updated: {}
            \n status_updated: {}\n county: {}\n county_status: {}\n manual_status: {}\naddress: {}
            \nResponse Time: {}",
            record.permit_link,
            record.permit_number,
            record.client,
            record.opened,
            record.last_updated,
            record.status_updated,
            record.county,
            record.county_status,
            record.manual_status,
            record.address,
            duration.as_micros()
        )));
    }

    Ok(HttpResponse::Ok().body("No record Found"))
}

#[get("/read-records-by-opened-date")]
pub async fn read_records_by_opened_date(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    dates: web::Json<HashMap<String, String>>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let rtxn = handle!(db_env.env.read_txn());

    if let Some(start_date) = dates.get("start_date") {
        if let Some(end_date) = dates.get("end_date") {
            let start_date = NaiveDate::parse_from_str(&start_date, "%Y-%m-%d")
                .expect("Invalid start_date format, expected YYYY-MM-DD");
            let end_date = NaiveDate::parse_from_str(&end_date, "%Y-%m-%d")
                .expect("Invalid end_date format, expected YYYY-MM-DD");
            let mut dates = vec![];
            let mut current_date = start_date;
            let mut records = vec![];

            while current_date <= end_date {
                dates.push(current_date.format("%Y-%m-%d").to_string());
                current_date += chrono::Duration::days(1);
            }

            for date in dates {
                let mut cursor = db_handles.db_data.main_db.prefix_iter(&rtxn, &date)?;

                while let Some(Ok((key, value))) = cursor.next() {
                    records.push((key, value))
                }
            }

            let duration = start.elapsed().as_micros();

            let response = json!({
                "Response Time": duration,
                "Data": records
            });

            return Ok(HttpResponse::Ok().json(response));
        }
    }

    Ok(HttpResponse::Ok().body("Both start_date and end_date must exist"))
}

#[get("/read-permits-with-filter")]
pub async fn read_permit_with_filter(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    filter_data: Option<web::Json<HashMap<String, String>>>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();
    let rtxn = handle!(db_env.env.read_txn());

    if let Some(filter_data) = filter_data {
        if let Some(start_date) = filter_data.get("start_date") {
            if let Some(end_date) = filter_data.get("end_date") {
                let start_date = NaiveDate::parse_from_str(&start_date, "%Y-%m-%d")
                    .expect("Invalid start_date format, expected YYYY-MM-DD");
                let end_date = NaiveDate::parse_from_str(&end_date, "%Y-%m-%d")
                    .expect("Invalid end_date format, expected YYYY-MM-DD");
                let mut dates = vec![];
                let mut current_date = start_date;
                let mut records = vec![];

                while current_date <= end_date {
                    dates.push(current_date.format("%Y-%m-%d").to_string());
                    current_date += chrono::Duration::days(1);
                }

                for date in dates {
                    let mut cursor = db_handles.db_data.main_db.prefix_iter(&rtxn, &date)?;

                    while let Some(Ok((key, value))) = cursor.next() {
                        records.push((key, value))
                    }
                }

                let county = match filter_data.get("county") {
                    Some(county) => county,
                    None => "",
                };
                let county_status = match filter_data.get("county_status") {
                    Some(county_status) => county_status,
                    None => "",
                };
                let client = match filter_data.get("client") {
                    Some(client) => client,
                    None => "",
                };

                if county.is_empty() && county_status.is_empty() && client.is_empty() {
                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": records
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if !county.is_empty() && county_status.is_empty() && client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.county == county {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if county.is_empty() && !county_status.is_empty() && client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.county_status.to_string() == county_status {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if county.is_empty() && county_status.is_empty() && !client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.client == client {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if !county.is_empty() && !county_status.is_empty() && client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.county == county
                            && item.1.county_status.to_string() == county_status
                        {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if county.is_empty() && !county_status.is_empty() && !client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.client == client
                            && item.1.county_status.to_string() == county_status
                        {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if !county.is_empty() && county_status.is_empty() && !client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.county == county && item.1.client == client {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                if !county.is_empty() && !county_status.is_empty() && !client.is_empty() {
                    let mut final_results = vec![];
                    for item in records {
                        if item.1.county == county
                            && item.1.county_status.to_string() == county_status
                            && item.1.client == client
                        {
                            final_results.push((item.0, item.1))
                        }
                    }

                    let duration = start.elapsed().as_micros();
                    let response = json!({
                        "Response Time": duration,
                        "Data": final_results
                    });

                    return Ok(HttpResponse::Ok().json(response));
                }

                return Ok(HttpResponse::Ok().body("No Records Found"))
            }
        }
    }

    Ok(HttpResponse::Ok().body("Both start_date and end_date must exist"))
}

#[get("/read-record")]
pub async fn read_record(
    db: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    query: Option<web::Json<HashMap<String, String>>>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let start = std::time::Instant::now();

    let rtxn = handle!(db.env.read_txn());
    let main_db = db_handles.db_data.main_db;
    let cursor = handle!(main_db.iter(&rtxn));
    let stats = handle!(main_db.stat(&rtxn));
    let entries = stats.entries;

    if let Some(query) = query {
        if query.is_empty() {
            let records: Vec<(String, DBSchema)> = cursor
                .take(50)
                .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                .collect();

            let duration = start.elapsed();
            let response = data_with_response_time(duration, records, entries);

            return Ok(HttpResponse::Ok().json(response));
        }

        let page = match query.get("page") {
            Some(page) => page,
            None => "",
        };
        let county = match query.get("county") {
            Some(county) => county,
            None => "",
        };
        let client = match query.get("client") {
            Some(client) => client,
            None => "",
        };
        let status = match query.get("county_status") {
            Some(status) => status,
            None => "",
        };
        let pagination = match query.get("records_per_page") {
            Some(pagination) => pagination,
            None => "",
        };
        let sort = match query.get("sort") {
            Some(sorted) => sorted,
            None => "",
        };
        let sort_key = match query.get("sort_key") {
            Some(sort_key) => sort_key,
            None => "",
        };

        if county.is_empty() && client.is_empty() && status.is_empty() {
            if !page.is_empty() {
                let stats = handle!(main_db.stat(&rtxn));
                let entires = stats.entries;
                let mut page: usize = match page.parse() {
                    Ok(num) => num,
                    Err(_) => {
                        return Ok(HttpResponse::Ok()
                            .body("Enter a valid page number. It must be an integer."));
                    }
                };

                if pagination.is_empty() {
                    let page_in_db = entires / 50;
                    if sort.is_empty() || sort == "asc" {
                        if entires < page * 50 {
                            return Ok(HttpResponse::Ok()
                                .body(format!("The DB only has {page_in_db} Pages")));
                        }

                        if page == 1 {
                            page -= 1
                        }

                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(page * 50)
                            .take(50)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    } else {
                        if entires < page * 50 {
                            return Ok(HttpResponse::Ok()
                                .body(format!("The DB only has {page_in_db} Pages")));
                        }

                        page = page_in_db - page - 1;
                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(page * 50)
                            .take(50)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    }
                } else if !pagination.is_empty() {
                    let pagination = match pagination.parse() {
                        Ok(num) => num,
                        Err(_) => {
                            return Ok(HttpResponse::Ok()
                                .body("Enter a valid page number. It must be an integer."));
                        }
                    };
                    let page_in_db = entires / pagination;

                    if sort.is_empty() || sort == "asc" {
                        if entires < page * pagination {
                            return Ok(HttpResponse::Ok()
                                .body(format!("The DB only has {page_in_db} Pages")));
                        }

                        if page == 1 {
                            page -= 1
                        }

                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(page * pagination)
                            .take(pagination)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entries);

                        return Ok(HttpResponse::Ok().json(response));
                    } else {
                        if entires < page * pagination {
                            return Ok(HttpResponse::Ok()
                                .body(format!("The DB only has {page_in_db} Pages")));
                        }

                        page = page_in_db - page - 1;
                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(page * pagination)
                            .take(pagination)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    }
                }
            } else if page.is_empty() {
                if !pagination.is_empty() {
                    let stats = handle!(main_db.stat(&rtxn));
                    let entires = stats.entries;
                    let pagination = match pagination.parse() {
                        Ok(num) => num,
                        Err(_) => {
                            return Ok(HttpResponse::Ok()
                                .body("Enter a valid page number. It must be an integer."));
                        }
                    };

                    let page_in_db = entires / pagination;
                    if sort.is_empty() || sort == "asc" {
                        let records: Vec<(String, DBSchema)> = cursor
                            .take(pagination)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    } else {
                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(page_in_db - pagination)
                            .take(pagination)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    }
                } else if pagination.is_empty() {
                    let stats = handle!(main_db.stat(&rtxn));
                    let entires = stats.entries;

                    if sort.is_empty() || sort == "asc" {
                        let records: Vec<(String, DBSchema)> = cursor
                            .take(50)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    } else {
                        let skip = entires - 50;
                        let records: Vec<(String, DBSchema)> = cursor
                            .skip(skip)
                            .take(50)
                            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
                            .collect();

                        let duration = start.elapsed();
                        let response = data_with_response_time(duration, records, entires);

                        return Ok(HttpResponse::Ok().json(response));
                    }
                }
            }
        }

        if !county.is_empty() && !client.is_empty() && !status.is_empty() {
            let status = match Status::from_str(status) {
                Ok(stat) => stat,
                Err(_) => {
                    return Ok(HttpResponse::Ok().body("The given status is not valid."));
                }
            };
            let key = KeySchema {
                county: county.to_string(),
                client: client.to_string(),
                county_status: status,
            };

            let set = handle!(db_handles.db_data.composite_index.get(&rtxn, &key));

            if let Some(set) = set {
                let mut records =
                    match helper_function_for_retrieving_data(&set, &rtxn, &db_handles) {
                        Ok(records) => records,
                        Err(_) => return Ok(HttpResponse::Ok().body("Failed to retrieve records")),
                    };

                let pagination: usize = match pagination.parse() {
                    Ok(pagination) => pagination,
                    Err(_) => 0,
                };

                if (sort.is_empty() || sort == "asc") && sort_key.is_empty() {
                    records.sort_by_key(|schema| schema.opened);

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                } else if sort == "dsc" && sort_key.is_empty() {
                    records.sort_by_key(|schema| std::cmp::Reverse(schema.opened));

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                }

                if (sort.is_empty() || sort == "asc") && sort_key == "opened" {
                    records.sort_by_key(|schema| schema.opened);

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                } else if sort == "dsc" && sort_key == "opened" {
                    records.sort_by_key(|schema| std::cmp::Reverse(schema.opened));

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                }

                if (sort.is_empty() || sort == "asc") && sort_key == "last_updated" {
                    records.sort_by_key(|schema| schema.last_updated);

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                } else if sort == "dsc" && sort_key == "last_updated" {
                    records.sort_by_key(|schema| std::cmp::Reverse(schema.last_updated));

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                }

                if (sort.is_empty() || sort == "asc") && sort_key == "status_updated" {
                    records.sort_by_key(|schema| schema.status_updated);

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                } else if sort == "dsc" && sort_key == "status_updated" {
                    records.sort_by_key(|schema| std::cmp::Reverse(schema.status_updated));

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                }

                if (sort.is_empty() || sort == "asc") && sort_key == "manual_status" {
                    records.sort_by_key(|schema| schema.manual_status.clone());

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                } else if sort == "dsc" && sort_key == "manual_status" {
                    records.sort_by_key(|schema| std::cmp::Reverse(schema.manual_status.clone()));

                    if pagination != 0 {
                        if let Some(slice) = records.get(..pagination) {
                            let duration = start.elapsed();
                            let response =
                                data_with_response_time_for_slice(duration, slice, set.len());

                            return Ok(HttpResponse::Ok().json(response));
                        }
                    }
                    let duration = start.elapsed();
                    let response = data_with_response_time_for_slice(duration, &records, set.len());

                    return Ok(HttpResponse::Ok().json(response));
                }
            }
            return Ok(HttpResponse::Ok().body("No Records Found"));
        }
    } else {
        let records: Vec<(String, DBSchema)> = cursor
            .take(50)
            .filter_map(|res| res.ok().map(|(key, value)| (key.to_string(), value)))
            .collect();

        let duration = start.elapsed();
        let response = data_with_response_time(duration, records, entries);

        return Ok(HttpResponse::Ok().json(response));
    }

    Ok(HttpResponse::Ok().body("Couldn't read From DataBase"))
}

fn helper_function_for_retrieving_data(
    set: &HashSet<String>,
    rtxn: &RoTxn,
    db_handles: &web::Data<DBdata>,
) -> Result<Vec<DBSchema>, Box<dyn std::error::Error>> {
    let mut storage: Vec<DBSchema> = vec![];

    for each in set {
        let value = handle!(db_handles.db_data.main_db.get(rtxn, each));

        if let Some(value) = value {
            storage.push(value);
        } else {
            println!("couldn't find the value for uuid: {each}")
        }
    }
    Ok(storage)
}

#[put("/update-record/{uuid}")]
pub async fn update_records(
    db_env: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    path: web::Path<String>,
    updated_data: web::Json<UpdateDBSchema>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let uuid = path.into_inner();

    let main_record = handle!(db_handles.db_data.main_db.get(&wtxn, &uuid));

    if let Some(mut data) = main_record {
        let key = KeySchema {
            county: data.county.to_owned(),
            client: data.client.to_owned(),
            county_status: data.county_status.clone(),
        };

        let set = handle!(db_handles.db_data.composite_index.get(&wtxn, &key));
        if let Some(mut set) = set {
            set.remove(&uuid.clone());
            handle!(
                db_handles
                    .db_data
                    .composite_index
                    .put(&mut wtxn, &key, &mut set)
            );
        } else {
            return Ok(HttpResponse::Ok().body("The UUID is not Valid".to_string()));
        }

        if let Some(permit_link) = &updated_data.permit_link {
            data.permit_link = permit_link.to_string();
        }
        if let Some(permit_number) = &updated_data.permit_number {
            data.permit_number = permit_number.to_string();
        }
        if let Some(client) = &updated_data.client {
            data.client = client.to_string();
        }
        if let Some(county) = &updated_data.county {
            data.county = county.to_string();
        }
        if let Some(manual_status) = &updated_data.manual_status {
            data.manual_status = manual_status.clone()
        }
        if let Some(county_status) = &updated_data.county_status {
            data.county_status = county_status.clone()
        }
        if let Some(address) = &updated_data.address {
            data.address = address.to_string();
        }

        let format = "%Y-%m-%dT%H:%M:%S%.3f";
        if let Some(last_updated) = &updated_data.last_updated {
            match NaiveDateTime::parse_from_str(last_updated, format) {
                Ok(naive_dt) => data.last_updated = naive_dt,
                Err(_) => {
                    return Ok(HttpResponse::Ok().body("The format for last_updated is wrong\nEnsure you are using this format: %Y-%m-%dT%H:%M:%S%.3f"))
                }
            }
        }
        if let Some(status_updated) = &updated_data.status_updated {
            match NaiveDateTime::parse_from_str(status_updated, format) {
                Ok(naive_dt) => data.status_updated = naive_dt,
                Err(_) => {
                    return Ok(HttpResponse::Ok().body("The format for status_updated is wrong\nEnsure you are using this format: %Y-%m-%dT%H:%M:%S%.3f"))
                }
            }
        }

        let new_key = KeySchema {
            county: data.county.to_owned(),
            county_status: data.county_status.to_owned(),
            client: data.client.to_owned(),
        };

        let new_set = handle!(db_handles.db_data.composite_index.get(&wtxn, &new_key));

        if let Some(mut new_set) = new_set {
            new_set.insert(uuid.clone());
            handle!(
                db_handles
                    .db_data
                    .composite_index
                    .put(&mut wtxn, &new_key, &mut new_set)
            );
        } else {
            let mut new_set: HashSet<String> = HashSet::new();
            new_set.insert(uuid.clone());
            handle!(
                db_handles
                    .db_data
                    .composite_index
                    .put(&mut wtxn, &new_key, &mut new_set)
            );
        }

        handle!(
            db_handles
                .db_data
                .main_db
                .put(&mut wtxn, &uuid, &data.clone())
        );

        let start = std::time::Instant::now();
        handle!(wtxn.commit());
        let duration = start.elapsed();

        return Ok(HttpResponse::Ok().body(format!(
            "Successfully Updated the Record\nResponse Time: {}",
            duration.as_micros()
        )));
    }
    Ok(HttpResponse::Ok().body("Failed to update the Record\nNo Record Exists"))
}

#[put("/update-processing-status/{permit_number}/{last_updated}")]
pub async fn update_processing_status(
    db_env: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    path: web::Path<(String, String)>,
    updated_data: web::Json<UpdateProcessingStatusSchema>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let path = path.into_inner();
    let mut key = format!("{}-{}", path.0, path.1);

    let record = handle!(db_handles.db_data.processing_state.get(&mut wtxn, &key));

    if let Some(mut record) = record {
        if let Some(processing_status) = updated_data.processing_status.to_owned() {
            record.processing_status = processing_status
        }
        if let Some(due_date) = updated_data.due_date {
            record.due_date = due_date
        }
        if let Some(assigned) = updated_data.assigned_to.to_owned() {
            record.assigned_to = assigned
        }
        if let Some(last_modified) = updated_data.last_modified {
            record.last_modified = last_modified;
            handle!(db_handles.db_data.processing_state.delete(&mut wtxn, &key));
            key = format!("{}-{:?}", path.0, last_modified);
        }

        handle!(
            db_handles
                .db_data
                .processing_state
                .put(&mut wtxn, &key, &record)
        );
    } else {
        return Ok(HttpResponse::Ok().body(format!(
            "No record found with the permit number: {}",
            path.0
        )));
    }

    let start = std::time::Instant::now();
    handle!(wtxn.commit());
    let duration = start.elapsed().as_micros();

    Ok(HttpResponse::Ok().body(format!(
        "Successfully updated the processing state\nResponse Time: {duration}"
    )))
}

#[put("/update-payment-details/{permit_number}/{date}")]
pub async fn update_payment_details(
    db_handles: web::Data<DBdata>,
    db_env: web::Data<DbEnv>,
    path: web::Path<(String, String)>,
    updated_data: web::Json<UpdatePayment>,
) -> Result<impl Responder, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let path = path.into_inner();
    let mut key = format!("{}-{}", path.0, path.1);

    let record = handle!(db_handles.db_data.payments_db.get(&wtxn, &key));

    if let Some(mut record) = record {
        if let Some(payment) = updated_data.payment.to_owned() {
            record.payment = payment;
        }
        if let Some(status) = updated_data.status.to_owned() {
            record.status = status;
        }
        if let Some(amount) = updated_data.amount {
            record.amount = amount
        }
        if let Some(date) = updated_data.date {
            record.date = date;
            handle!(db_handles.db_data.payments_db.delete(&mut wtxn, &key));
            println!("{key}");
            key = format!("{}-{:?}", path.0, date);
            println!("{key}");
        }
        handle!(db_handles.db_data.payments_db.put(&mut wtxn, &key, &record));
    } else {
        return Ok(HttpResponse::Ok().body(format!(
            "No record found with the permit number: {}",
            path.0
        )));
    }

    let start = std::time::Instant::now();
    handle!(wtxn.commit());
    let duration = start.elapsed().as_micros();

    Ok(HttpResponse::Ok().body(format!(
        "Successfully updated the processing state\nResponse Time: {duration}"
    )))
}

#[delete("/delete-record/{uuid}")]
pub async fn delete_record(
    db_env: web::Data<DbEnv>,
    db_handles: web::Data<DBdata>,
    path: web::Path<String>,
) -> Result<String, Box<dyn std::error::Error>> {
    let mut wtxn = handle!(db_env.env.write_txn());
    let uuid = path.into_inner();

    let main_data = handle!(db_handles.db_data.main_db.get(&wtxn, &uuid));

    if let Some(record) = main_data {
        let key = KeySchema {
            county: record.county.to_owned(),
            client: record.client.to_owned(),
            county_status: record.county_status.clone(),
        };

        let set = handle!(db_handles.db_data.composite_index.get(&wtxn, &key));
        if let Some(mut set) = set {
            set.remove(&uuid.clone());
            handle!(
                db_handles
                    .db_data
                    .composite_index
                    .put(&mut wtxn, &key, &set)
            );
        } else {
            return Ok("The UUID is not in the DataBase".to_string());
        }

        handle!(db_handles.db_data.main_db.delete(&mut wtxn, &uuid));

        let start = std::time::Instant::now();
        handle!(wtxn.commit());
        let duration = start.elapsed();

        return Ok(format!(
            "Successfully Deleted the record\nResponse Time: {}",
            duration.as_micros()
        ));
    } else {
        return Ok("Couldn't Delete the record".to_string());
    }
}

#[get("/load-the-db")]
async fn load_the_db() -> Result<impl Responder, Box<dyn std::error::Error>> {
    match loader().await {
        Ok(_) => Ok(HttpResponse::Ok().body("Successfully Loaded the records")),
        Err(_) => Ok(HttpResponse::Ok().body("Failed to Load the Data")),
    }
}