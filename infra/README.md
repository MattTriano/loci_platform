# Infra

## AWS Auth

First, define profiles in `~/.aws/config` that define the account and role to use when invoking that profile.

Then you can authenticate the `AWS CLI` for 12 hours via this command. Open the link and enter the device code when you can.

```console
aws sso login --profile <dev_prodfile_name> --use-device-code
```

## Terraform commands

* `terraform init`: downloads providers and prepares project
* `terraform show`: Shows a full output of the things in the terraform state.
* `terraform state list`: shows a narrower set resources managed by a terraform project.
* `terraform plan`: plans out the changes needed to move the current state to the desired state.
* `terraform help`: self explanatory
* `terraform fmt`: format terraform files.

# Terraform State

## Bootstrapping tfstate management

We want to use `OpenTofu` to manage our resources, but we also need an S3 bucket and DynamoDB table to provide a backend for `OpenTofu` to store and lock state. So we'll have a one-time initialization step to create those.

```console
cd bootstrap/dev
AWS_PROFILE=dev_profile_name tofu init
AWS_PROFILE=dev_profile_name tofu apply

cd ../prod
AWS_PROFILE=prod_profile_name tofu init
AWS_PROFILE=prod_profile_name tofu apply
```

## Regular operation

In the top-level dir, any time you're switching between the `dev` or `prod` envs, you'll have to reconfigure the state. For example, if you've been building to `prod` but want to switch to building to `dev`, you'd have to run this command.

```console
$ AWS_PROFILE=dev_profile_name tofu init -var-file=dev.tfvars -reconfigure
```

NOTE: At present, `{dev,prod}.tfvars` just includes `environment = "dev"` (or prod).

Then you can work with the correct state.

```console
$ AWS_PROFILE=dev_profile_name tofu plan -var-file=dev.tfvars
$ AWS_PROFILE=dev_profile_name tofu apply -var-file=dev.tfvars
```

This is a temporary workflow (with respect to any non-dev usage); CI/CD will be set up to control adjusting prod infrastructure. It's already governed by IAM means.

