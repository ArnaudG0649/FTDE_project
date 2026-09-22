terraform {
  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "7.44.0"
    }
  }
}

provider "google" {
  project = var.project
  region  = var.region
}

resource "google_storage_bucket" "bucket" {
  name          = var.gcs_bucket_name
  location      = var.location
  force_destroy = true
}


resource "google_bigquery_dataset" "dataset" {
  dataset_id = var.bq_dataset_name
  location   = var.location
}


resource "google_bigquery_dataset" "dataset_2" {
  dataset_id = var.bq_dataset_name_2
  location   = var.location
}