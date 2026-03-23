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

cd ../staging
AWS_PROFILE=staging_profile_name tofu init
AWS_PROFILE=staging_profile_name tofu apply

cd ../prod
AWS_PROFILE=prod_profile_name tofu init
AWS_PROFILE=prod_profile_name tofu apply
```

## Regular operation

In the top-level dir, any time you're switching between the `dev`, `staging`, or `prod` envs, you'll have to reconfigure the state. For example, if you've been building to `prod` but want to switch to building to `dev`, you'd have to run this command.

```console
$ AWS_PROFILE=dev_profile_name tofu init -var-file=dev.tfvars -reconfigure
```

NOTE: At present, `{dev,prod}.tfvars` just includes `environment = "dev"` (or staging or prod) and `base_domain = "<your_domain>.com|org|etc"`.

Then you can work with the correct state.

```console
$ AWS_PROFILE=dev_profile_name tofu plan -var-file=dev.tfvars
$ AWS_PROFILE=dev_profile_name tofu apply -var-file=dev.tfvars
```

This is a temporary workflow (with respect to any non-dev usage); CI/CD will be set up to control adjusting prod infrastructure. It's already governed by IAM means.

## Deploying to a new environment

### 1. Apply the OpenTofu config

```bash
cd infra/
AWS_PROFILE=<env-profile> tofu init -reconfigure
AWS_PROFILE=<env-profile> tofu plan -var-file=<env>.tfvars
AWS_PROFILE=<env-profile> tofu apply -var-file=<env>.tfvars
```

The apply will pause at the ACM certificate validation step. This is expected — it
won't complete until you finish step 2.

### 2. Delegate the subdomain (while apply is still running)

The env's hosted zone (e.g. `dev.missinglastmile.net`) is created early in the apply,
but the mgmt account doesn't know about it yet. You need to add NS delegation manually
so that AWS can resolve the certificate validation DNS record.

1. Open the **env account's** Route53 console
2. Go to the `<env>.missinglastmile.net` hosted zone
3. Copy the 4 NS record values
4. Open the **mgmt account's** Route53 console
5. Go to the `missinglastmile.net` hosted zone
6. Create a new record:
   - Record name: `<env>` (e.g. `dev`)
   - Type: NS
   - Value: paste the 4 nameservers, one per line
   - TTL: 300
7. Save the record

After a few minutes, the ACM certificate will validate and the apply will continue.
CloudFront distribution creation takes another 5-10 minutes after that.

### 3. Verify

Once the apply completes, verify the setup:

```bash
# Check DNS resolution (may take a few minutes after apply)
dig A bike-map.<env>.missinglastmile.net

# Check CloudFront is serving (403 is expected with an empty bucket)
curl -I https://bike-map.<env>.missinglastmile.net
```

## Notes

- The NS delegation is a one-time step per environment. Future applies won't need it.
- If DNS doesn't resolve immediately, it may be negative caching from earlier lookups.
  Query the authoritative nameserver directly to confirm:
  `dig A bike-map.<env>.missinglastmile.net @<nameserver>`
- ACM certificate validation can take 2-30 minutes depending on DNS propagation timing.

## Outputs

After a successful apply, these outputs are available:

- `bike_map_url` — the public URL (e.g. `https://bike-map.dev.missinglastmile.net`)
- `dns_zone_name_servers` — the NS records (needed for step 2 on first deploy)

You can see outputs via

```console
AWS_PROFILE=dev_profile_name tofu output -json --var-file=dev.tfvars
```