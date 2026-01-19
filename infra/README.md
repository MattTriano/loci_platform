
```console
AWS_PROFILE=sandbox-dev-senior-dev-developer terraform apply
```

## Terraform commands

* `terraform init`: downloads providers and prepares project
* `terraform show`: Shows a full output of the things in the terraform state.
* `terraform state list`: shows a narrower set resources managed by a terraform project.
* `terraform plan`: plans out the changes needed to move the current state to the desired state.
* `terraform help`: self explanatory
* `terraform fmt`: format terraform files.

## Terraform State



## AWS Misc

aws s3api head-bucket --bucket loci_infra_dev 2>&1

aws s3api head-bucket --bucket loci_infra_dev --profile sandbox-dev-senior-dev


# Usage

First, comment out the `backend{}` from the `providers.tf` and run
`AWS_PROFILE=profile_name terraform init --backend-config="dev.s3.tfbackend" -var="environment=dev"`

then

`AWS_PROFILE=profile_name terraform plan -var="environment=dev"`
`AWS_PROFILE=profile_name terraform apply -var="environment=dev"`

then uncomment the `backend{}` bit and run
`AWS_PROFILE=profile_name terraform init --backend-config="dev.s3.tfbackend" -var="environment=dev" --migrate-state`