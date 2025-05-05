use core::fmt;
use std::{collections::HashSet, str::FromStr, sync::Arc};
use chrono::NaiveDateTime;
use heed::{types::*, Database, Env};
use serde::{Deserialize, Serialize};

pub struct DbEnv {
   pub env: Arc<Env>,
}

pub struct DBdata {
   pub db_data: Arc<DBHandles>,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum Status {
    Active,
    Inactive,
    Pending,
    Closed,
    UnderReview
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            Status::Active => "Active",
            Status::Closed => "Closed",
            Status::Pending => "Pending",
            Status::Inactive => "Inactive",
            Status::UnderReview => "UnderReview"
        };
        write!(f, "{}", s)
    }
}

impl FromStr for Status {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "active" => Ok(Status::Active),
            "closed" => Ok(Status::Closed),
            "pending" => Ok(Status::Pending),
            "inactive" => Ok(Status::Inactive),
            "underreview" => Ok(Status::UnderReview),
            _ => Err(())
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct DBSchema {
    pub permit_link: String,
    pub permit_number: String,
    pub client: String,
    pub opened: NaiveDateTime,
    pub last_updated: NaiveDateTime,
    pub status_updated: NaiveDateTime,
    pub county: String,
    pub county_status: Status,
    pub manual_status: Status,
    pub address: String
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct UpdateDBSchema {
    pub permit_link: Option<String>,
    pub permit_number: Option<String>,
    pub last_updated: Option<String>,
    pub status_updated: Option<String>,
    pub client: Option<String>,
    pub county: Option<String>,
    pub county_status: Option<Status>,
    pub manual_status: Option<Status>,
    pub address: Option<String> 
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, PartialOrd, Eq, Ord)]
pub enum ProcessStatus {
    ApprovedWithConditions,
    PendingAdditionalReview,
    RevisionsReceived
}

impl fmt::Display for ProcessStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = match self {
            ProcessStatus::ApprovedWithConditions => "ApprovedWithConditions",
            ProcessStatus::PendingAdditionalReview => "PendingAdditionalReview",
            ProcessStatus::RevisionsReceived => "RevisionsReceived"
        };
        write!(f, "{}", s)
    }
}

impl FromStr for ProcessStatus {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "approvedwithconditions" => Ok(ProcessStatus::ApprovedWithConditions),
            "pendingadditionalreview" => Ok(ProcessStatus::PendingAdditionalReview),
            "revisionsreceived" => Ok(ProcessStatus::RevisionsReceived),
            _ => Err(())
        }
    }
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct ProcessingStatusSchema {
    pub processing_status: ProcessStatus,
    pub due_date: NaiveDateTime,
    pub assigned_to: String,
    pub last_modified: NaiveDateTime
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct UpdateProcessingStatusSchema {
    pub processing_status: Option<ProcessStatus>,
    pub due_date: Option<NaiveDateTime>,
    pub assigned_to: Option<String>,
    pub last_modified: Option<NaiveDateTime>
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct Payments {
    pub payment: String,
    pub date: NaiveDateTime,
    pub amount: u64,
    pub status: String
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct UpdatePayment {
    pub payment: Option<String>,
    pub date: Option<NaiveDateTime>,
    pub amount: Option<u64>,
    pub status: Option<String> 
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub struct KeySchema {
    pub client: String,
    pub county: String,
    pub county_status: Status
}

#[derive(Clone)]
pub struct DBHandles {
    pub main_db: Database<Str, SerdeBincode<DBSchema>>,
    pub composite_index: Database<SerdeBincode<KeySchema>, SerdeBincode<HashSet<String>>>,
    pub processing_state: Database<Str, SerdeBincode<ProcessingStatusSchema>>,
    pub payments_db: Database<Str, SerdeBincode<Payments>>
}