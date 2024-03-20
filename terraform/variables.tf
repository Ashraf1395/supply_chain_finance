variable "location" {
  description = "My location"
  default = "US"
}

variable "bq_dataset_name"{
    description = "Bigquery Dataset name"
    default = "terraform_bigquery"
}

variable "gcs_storage_class" {
  description = "GCS storage class name"
  default = "raw_streaming"

}

variable "gcs_bucket_name" {
  description = "GCS storage bucket name"
  default = "supply-chain-data-terraform"
}

