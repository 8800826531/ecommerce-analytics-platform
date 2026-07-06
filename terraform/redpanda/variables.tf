variable "redpanda_bootstrap" {
  description = "Redpanda Cloud bootstrap server URL"
  type        = string
  sensitive   = true
}

variable "redpanda_username" {
  description = "Redpanda service account username"
  type        = string
  sensitive   = true
}

variable "redpanda_password" {
  description = "Redpanda service account password"
  type        = string
  sensitive   = true
}