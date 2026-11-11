variable "environment" {
  type = string

  validation {
    condition     = contains(["homolog", "prod"], var.environment)
    error_message = "environment deve ser homolog ou prod."
  }
}

variable "region" {
  type    = string
  default = "sa-east-1"
}

variable "path_pattern" {
  type    = string
  default = "/execution/*"
}

variable "listener_rule_priority" {
  type    = number
  default = 30
}
