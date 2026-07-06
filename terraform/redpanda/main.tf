terraform {
  required_providers {
    kafka = {
      source  = "Mongey/kafka"
      version = "~> 0.5"
    }
  }
}

provider "kafka" {
  bootstrap_servers = [var.redpanda_bootstrap]
  tls_enabled       = true

  sasl_username  = var.redpanda_username
  sasl_password  = var.redpanda_password
  sasl_mechanism = "scram-sha256"
}