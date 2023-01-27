locals {
  data_lake_bucket = "dzc-trietle-data-lake"
}

variable "project" {
  description = "GCP Project ID for DE Zoo Camp course"
  default = "dzc-trietle"
}

variable "region" {
  description = "Region for GCP resources. Choose as per your location: https://cloud.google.com/about/locations"
  default = "asia-southeast1"
  type = string
}

variable "storage_class" {
  description = "Storage class type for your bucket. Check official docs for more info."
  default = "STANDARD"
}

variable "BQ_DATASET" {
  description = "BigQuery Dataset that raw data (from GCS) will be written to"
  default = "trips_data_all"
  type = string
}