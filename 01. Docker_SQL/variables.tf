#reutilizacion de codigo para proyectos

variable "location" {
  description = "Project Location"
  default     = "US"
}

variable "bq_dataset_demo" {
  description = "My first BigQuery Dataset"
  default     = "demo_dataset"
}

variable "bq_bucket_demo" {
  description = "My Bucket name"
  default     = "encoded-ensign-412700-terra-bucket"
}

variable "gcs_storage_class" {
  description = "Bucket Storage Class"
  default     = "STANDARD"
}