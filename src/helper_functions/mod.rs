use std::sync::Arc;
use chrono::{NaiveDate, NaiveDateTime, NaiveTime};
use fake::{Fake, Faker};
use futures::stream::{FuturesUnordered, StreamExt};
use reqwest::Client;
use serde_json::{json, Value};
use crate::struct_definitions::{DBSchema, Payments, ProcessStatus, ProcessingStatusSchema, Status};
use tokio::{sync::Semaphore, task};
use rand::{seq::IndexedRandom, Rng};
use std::time::Duration;
use dotenv::dotenv;

const ARR: [Status; 5] = [
    Status::Active,
    Status::Closed,
    Status::Inactive,
    Status::Pending,
    Status::UnderReview,
];
const COUNTY: [&str; 5] = ["one", "two", "three", "four", "five"];
const CLIENT: [&str; 5] = ["a", "b", "c", "d", "e"];
const CONCURRENCY_LIMIT: usize = 1000;

pub fn random_naive_datetime() -> NaiveDateTime {
    let mut rng = rand::rng();

    let year = rng.random_range(2020..=2026);
    let month = rng.random_range(1..=12);
    let day = rng.random_range(1..=28);

    let hour = rng.random_range(0..24);
    let min = rng.random_range(0..60);
    let sec = rng.random_range(0..60);
    let millis = rng.random_range(0..1000);

    let date = NaiveDate::from_ymd_opt(year, month, day).unwrap();
    let time = NaiveTime::from_hms_milli_opt(hour, min, sec, millis).unwrap();

    NaiveDateTime::new(date, time)
}

pub async fn loader() -> Result<(), Box<dyn std::error::Error>> {
    dotenv().ok();

    let client = Arc::new(Client::new());
    let semaphore = Arc::new(Semaphore::new(CONCURRENCY_LIMIT));

    let mut tasks = FuturesUnordered::new();

    for _ in 0..5_000 {
        let permit = semaphore.clone().acquire_owned().await.unwrap();
        let client = client.clone();

        tasks.push(task::spawn(async move {
            let fake_data = DBSchema {
                permit_link: Faker.fake(),
                permit_number: Faker.fake(),
                client: CLIENT.choose(&mut rand::rng()).unwrap().to_string().clone(),
                opened: random_naive_datetime(),
                last_updated: random_naive_datetime(),
                status_updated: random_naive_datetime(),
                county: COUNTY.choose(&mut rand::rng()).unwrap().to_string().clone(),
                address: Faker.fake(),
                county_status: ARR.choose(&mut rand::rng()).unwrap().clone(),
                manual_status: ARR.choose(&mut rand::rng()).unwrap().clone(),
            };

            let url = std::env::var("HOST_URL").expect("URL must be set");
            let url = format!("http://{}/create-record", url);

            let res = client
                .post(url)
                .json(&fake_data)
                .send()
                .await;

            match res {
                Ok(response) => {
                    if let Ok(body) = response.text().await {
                        println!("{}", body);
                    } else {
                        eprintln!("Failed to read response body");
                    }
                }
                Err(e) => {
                    eprintln!("Request error: {}", e);
                }
            }

            for _ in 0..2 {
                let processing_status = [ProcessStatus::ApprovedWithConditions, ProcessStatus::PendingAdditionalReview, ProcessStatus::RevisionsReceived].choose(&mut rand::rng()).unwrap().clone();
                let due_date = random_naive_datetime();
                let assigned: String = Faker.fake();
                let last_modified = random_naive_datetime();
    
                let fake_processing_status = ProcessingStatusSchema {
                    processing_status,
                    due_date,
                    assigned_to: assigned,
                    last_modified
                };
    
                let url = std::env::var("HOST_URL").expect("URL must be set");
                let url = format!("http://{}/create-processing-status/{}", url, fake_data.permit_number);
    
                let pro_res = client
                    .post(url)
                    .json(&fake_processing_status)
                    .send()
                    .await;
    
                match pro_res {
                    Ok(response) => {
                        if let Ok(body) = response.text().await {
                            println!("{}", body);
                        } else {
                            eprintln!("Failed to read response body");
                        }
                    }
                    Err(e) => {
                        eprintln!("Request error: {}", e);
                    }
                }
            }

            for _ in 0..2 {
                let amount: u64 = rand::rng().random_range(10_000..100_000);
                let date = random_naive_datetime();
                let payment: String = Faker.fake();
                let status: String = Faker.fake();
    
                let fake_payment_data = Payments{
                    amount,
                    date,
                    payment,
                    status 
                };
    
                let url = std::env::var("HOST_URL").expect("URL must be set");
                let url = format!("http://{}/create-payment/{}", url, fake_data.permit_number);
    
                let pro_res = client
                    .post(url)
                    .json(&fake_payment_data)
                    .send()
                    .await;
    
                match pro_res {
                    Ok(response) => {
                        if let Ok(body) = response.text().await {
                            println!("{}", body);
                        } else {
                            eprintln!("Failed to read response body");
                        }
                    }
                    Err(e) => {
                        eprintln!("Request error: {}", e);
                    }
                }
            }



            drop(permit);
        }));
    }
    while let Some(_) = tasks.next().await {}
    Ok(())
}

pub fn data_with_response_time(duration: Duration, record: Vec<(String, DBSchema)>, total_records: usize) -> Value {
    let response = json!({
        "Response_time": duration.as_micros(),
        "Number_of_records": total_records,
        "data": record
    });

    response
}

pub fn data_with_response_time_for_slice(duration: Duration, record: &[DBSchema], total_records: usize) -> Value {
    let response = json!({
        "Response_time": duration.as_micros(),
        "Number_of_records": total_records,
        "data": record
    });

    response
}

