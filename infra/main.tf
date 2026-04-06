provider "google" {
  project     = var.project
  region      = var.region
  credentials = file(var.credentials_file)
}

# --- GCS buckets ---

resource "google_storage_bucket" "tf_state" {
  name                        = "american-beauty-institute-tf-state"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

resource "google_storage_bucket" "raw" {
  name                        = "american-beauty-institute-raw"
  location                    = var.region
  force_destroy               = false
  uniform_bucket_level_access = true

  versioning {
    enabled = true
  }
}

# --- Service account for Dagster VM ---

resource "google_service_account" "dagster" {
  account_id   = "dagster-daemon"
  display_name = "Dagster Daemon Service Account"
}

resource "google_project_iam_member" "dagster_bq_data_editor" {
  project = var.project
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

resource "google_project_iam_member" "dagster_bq_job_user" {
  project = var.project
  role    = "roles/bigquery.jobUser"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

resource "google_project_iam_member" "dagster_storage_admin" {
  project = var.project
  role    = "roles/storage.admin"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

resource "google_project_iam_member" "dagster_secret_accessor" {
  project = var.project
  role    = "roles/secretmanager.secretAccessor"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

# --- IAP SSH access ---

resource "google_project_iam_member" "dagster_iap_tunnel" {
  project = var.project
  role    = "roles/iap.tunnelResourceAccessor"
  member  = "serviceAccount:${google_service_account.dagster.email}"
}

resource "google_project_service" "iap" {
  service            = "iap.googleapis.com"
  disable_on_destroy = false
}

resource "google_project_service" "compute" {
  service            = "compute.googleapis.com"
  disable_on_destroy = false
}

# --- Firewall: allow IAP SSH ---

resource "google_compute_firewall" "allow_iap_ssh" {
  name    = "allow-iap-ssh"
  network = "default"

  allow {
    protocol = "tcp"
    ports    = ["22"]
  }

  source_ranges = ["35.235.240.0/20"]
  target_tags   = ["dagster-daemon"]
}

# --- Dagster VM ---

resource "google_compute_instance" "dagster" {
  name         = "dagster-daemon"
  machine_type = "e2-micro"
  zone         = var.zone
  tags         = ["dagster-daemon"]

  depends_on = [
    google_project_service.compute,
    google_project_service.iap,
  ]

  boot_disk {
    initialize_params {
      image = "debian-cloud/debian-12"
      size  = 30
    }
  }

  network_interface {
    network = "default"
    # No access_config block = no external IP, IAP only
  }

  service_account {
    email  = google_service_account.dagster.email
    scopes = ["cloud-platform"]
  }

  metadata = {
    startup-script = templatefile("${path.module}/files/startup.sh", {
      repo_url   = var.dagster_repo_url
      branch     = var.dagster_branch
    })
  }
}
