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



# Cross-Account DNS Role: `<domain_name>-dns-writer`

This role lives in the **admin-mgmt** AWS account and grants the **prod**
account permission to manage records in the `<domain_name>.com` Route53 hosted
zone.

## Why this exists

The apex zone for `<domain_name>.com` is owned by admin-mgmt (so domain ownership is centralized in one account). However, the bike-map prod infrastructure — CloudFront distribution, ACM certificate, Lambda — lives in the prod account, and these resources need DNS records (`A` records, ACM validation `CNAME`s) written into the apex zone.

Rather than manually copying records between accounts on every deploy, this role lets the prod tofu run write directly into admin-mgmt's zone via cross-account `sts:AssumeRole`.

For dev and staging, no cross-account role is needed: those envs have their own delegated zones (`dev.<domain_name>.com`, `staging.<domain_name>.com`) that live in the env account itself.

## Permissions (minimal)

The role can do four things, all scoped to one zone:

- `route53:ChangeResourceRecordSets` — create/update/delete records
- `route53:ListResourceRecordSets` — read records (for tofu state diffs)
- `route53:GetHostedZone` — read zone metadata
- `route53:GetChange` — poll change-propagation status

It cannot create or delete zones, cannot touch other zones, cannot touch any other AWS service. See `permissions-policy.json` for the exact JSON.

## Who can assume it

Only the prod SSO admin role (`AWSReservedSSO_AdministratorAccess_<suffix>` in the prod account).
The trust policy also requires an `ExternalId` of `<domain_name>-dns-writer-v1`. See `trust-policy.json`.

## Setup steps

These are one-time manual steps. They must be re-run if you fork this repo into a new AWS org, or if you want to apply the same pattern to a different domain.

### 1. Find your prod SSO admin role ARN

Authenticate to the prod account:

```bash
aws sso login --profile <admin-mgmt-profile> --use-device-code
aws sts get-caller-identity --profile <admin-mgmt-profile>
```

The `Arn` field will look like:

```
arn:aws:sts::PROD_ACCT_ID:assumed-role/AWSReservedSSO_AdministratorAccess_<suffix>/<your-email>
```

The role ARN you need is:

```
arn:aws:iam::PROD_ACCT_ID:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_<suffix>
```

(Note the `aws-reserved/sso.amazonaws.com/` path prefix — required for SSO-managed roles.)

### 2. Find the <domain_name>.com zone ID

Authenticate to admin-mgmt:

```bash
aws sso login --profile <admin-mgmt-profile> --use-device-code
aws route53 list-hosted-zones --profile  \
  --query "HostedZones[?Name=='<domain_name>.com.'].Id" --output text
```

Output: `/hostedzone/Z0123456789ABCDEFGHIJ`. The ID is the trailing part (`Z0123456789ABCDEFGHIJ`).

### 3. Edit the policy files

In `trust-policy.json`, replace the principal ARN with the prod SSO role ARN from step 1.

```console
cat > /tmp/trust-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowProdSSOAdminToAssume",
      "Effect": "Allow",
      "Principal": {
        "AWS": "arn:aws:iam::PROD_ACCT_ID:role/aws-reserved/sso.amazonaws.com/AWSReservedSSO_AdministratorAccess_<suffix>"
      },
      "Action": "sts:AssumeRole",
      "Condition": {
        "StringEquals": {
          "sts:ExternalId": "bikeinfra-dns-writer-v1"
        }
      }
    }
  ]
}
EOF
```

In `permissions-policy.json`, replace `<<domain_name>_ZONE_ID>` with the zone ID from step 2.

```console
cat > /tmp/permissions-policy.json <<'EOF'
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "ManageBikeinfraRecords",
      "Effect": "Allow",
      "Action": [
        "route53:ChangeResourceRecordSets",
        "route53:ListResourceRecordSets",
        "route53:GetHostedZone"
      ],
      "Resource": "arn:aws:route53:::hostedzone/Z0123456789ABCDEFGHIJ"
    },
    {
      "Sid": "PollChangeStatus",
      "Effect": "Allow",
      "Action": "route53:GetChange",
      "Resource": "arn:aws:route53:::change/*"
    }
  ]
}
EOF
```


### 4. Create the role and attach the policy

Authenticate to admin-mgmt:

```bash
cd path/to/this/directory

AWS_PROFILE=<admin-mgmt-profile> aws iam create-role \
  --role-name <domain_name>-dns-writer \
  --assume-role-policy-document file://trust-policy.json \
  --description "Allows prod account to manage <domain_name>.com DNS records"

AWS_PROFILE=<admin-mgmt-profile> aws iam put-role-policy \
  --role-name <domain_name>-dns-writer \
  --policy-name <domain_name>-dns-write \
  --policy-document file://permissions-policy.json
```

Capture the role ARN:

```bash
aws iam get-role --role-name <domain_name>-dns-writer --query 'Role.Arn' --output text
```

This is what you'll pass to tofu as `admin_mgmt_dns_role_arn` in `prod.tfvars`.

### 5. Verify

From prod credentials, confirm the role can be assumed:

```bash
aws sts assume-role \
  --profile  \
  --role-arn arn:aws:iam::ADMIN_MGMT_ACCT_ID:role/<domain_name>-dns-writer \
  --role-session-name verify \
  --external-id <domain_name>-dns-writer-v1
```

If this returns temporary credentials, setup is complete. If it returns "not authorized to perform sts:AssumeRole," the principal in the trust policy doesn't match the prod SSO role ARN — recheck step 1.

## Modifying the role later

To change permissions, edit `permissions-policy.json` and re-run:

```bash
aws iam put-role-policy \
  --role-name <domain_name>-dns-writer \
  --policy-name <domain_name>-dns-write \
  --policy-document file:///tmp/permissions-policy.json
```

(`put-role-policy` replaces the existing inline policy of the same name.)

To change who can assume the role, edit `trust-policy.json` and re-run:

```bash
aws iam update-assume-role-policy \
  --role-name <domain_name>-dns-writer \
  --policy-document file:///tmp/trust-policy.json
```

## Removing the role

If you ever want to tear this down:

```bash
aws iam delete-role-policy --role-name <domain_name>-dns-writer --policy-name <domain_name>-dns-write
aws iam delete-role --role-name <domain_name>-dns-writer
```