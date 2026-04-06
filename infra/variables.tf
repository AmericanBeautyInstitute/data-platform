variable "project" {
  description = "GCP project ID."
  type        = string
  default     = "american-beauty-institute"
}

variable "region" {
  description = "GCP region for all resources."
  type        = string
  default     = "us-east1"
}

variable "zone" {
  description = "GCP zone for the Dagster VM."
  type        = string
  default     = "us-east1-b"
}

variable "credentials_file" {
  description = "Path to the GCP service account JSON key file."
  type        = string
}

variable "dagster_repo_url" {
  description = "Git repository URL to clone onto the VM."
  type        = string
  default     = "https://github.com/americanbeautyinstitute/data-platform.git"
}

variable "dagster_branch" {
  description = "Git branch to check out on the VM."
  type        = string
  default     = "main"
}
