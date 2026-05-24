# Security and cost-control runbook

This document is the operational reference for the security and cost-control layers in this infrastructure. It covers what's protecting what, how to use the things that need using, and what to do when something fires. For the design rationale behind each layer, see the `issues/` folder for the original change writeups.

## Defense layers

The request path is a series of progressively narrower filters, each protecting a more expensive resource than the previous one:

1. **CloudFront + WAF rate-based rule** (prod only). Rejects a single IP that's hammering. Cheapest layer, runs first.
2. **CloudFront Function — basic auth** (dev/staging only). Rejects requests to the static sites without valid credentials. Microseconds, no API call downstream.
3. **CloudFront Function — API-key validation** (all envs, routing API only). Rejects requests without a valid `X-Api-Key` header. Microseconds, no Lambda invocation.
4. **API Gateway throttle**. Caps aggregate request rate per stage as a backstop.
5. **Lambda timeout (8s)** and the **account-level Lambda concurrency limit (10)**. Caps per-invocation and aggregate compute.
6. **AWS Budgets kill switch**. Catches anything the above layers miss, including bugs and AWS-side surprises.

Each layer is cheaper to run than the next, and each protects a more expensive resource. Multiple layers exist so any single one can fail or be misconfigured and you still have the others.

## Common operations

### Retrieve dev/staging basic auth credentials

Used when loading any non-prod URL in a browser.

```bash
aws ssm get-parameter \
  --name /loci-infra/dev/non-prod-auth/credentials \
  --with-decryption --region us-east-2 \
  --query 'Parameter.Value' --output text
```

Returns `bikeinfra:<24-char-password>`. Username is `bikeinfra`; the password is what comes after the colon. Browser prompts ask for them separately.

Swap `dev` for `staging` as appropriate.

### Retrieve the routing API key for a city

Used when manually exercising a routing API endpoint.

```bash
aws ssm get-parameter \
  --name /loci-infra/dev/<city>/bike-map/routing-api-key \
  --with-decryption --region us-east-2 \
  --query 'Parameter.Value' --output text
```

The key is also embedded in each per-city site's `config.json`, served from S3 — the SSM value is the source of truth, and the CloudFront Function reads from SSM via a Terraform data source.

### Test a routing API end-to-end

```bash
KEY=$(aws ssm get-parameter \
  --name /loci-infra/dev/chicago/bike-map/routing-api-key \
  --with-decryption --region us-east-2 \
  --query 'Parameter.Value' --output text)

curl -i -X POST https://routing-api.chicago.dev.bikeinfra.com/route \
  -H "Content-Type: application/json" \
  -H "X-Api-Key: $KEY" \
  -d '{"origin": [...], "destination": [...]}'
```

A wrong key returns 403 from the CloudFront Function (look for `x-cache: FunctionGeneratedResponse from cloudfront`). A correct key reaches the Lambda.

## When something fires

### Budget kill switch fired (got the "Cost guard kill switch fired" email)

The API Gateway stages in that environment are now throttling to zero. Site (static content) is unaffected; routing API and route-logger return 429 to all callers.

**Investigate first, restore second.**

1. Check Cost Explorer for which service drove the spend.
2. Check CloudWatch for the relevant metrics (Lambda Invocations, API Gateway Count, S3 PutRequests on the route-logger bucket).
3. If it's an attack, decide whether to leave the kill switch in place while you add additional defenses, or restore and rely on the existing layers.
4. To restore, from the affected environment's directory:

   ```bash
   cd /loci_platform/infra
   tofu apply --var-file=<env>.tfvars
   ```

   The plan should show ~20 API Gateway stage updates resetting throttle limits (10 routing APIs + 10 route-loggers). Apply only if the plan matches that expectation.

### Lambda throttle alarm fired

The routing Lambda in some city hit the account-level concurrency cap. Either real traffic outgrew the cap (legitimate growth) or someone is generating sustained load.

1. Check the `ConcurrentExecutions` and `Throttles` metrics for the affected city's function in CloudWatch.
2. Cross-reference timestamps with WAF blocked-request metrics (prod) or API Gateway 4xx rate.
3. If legitimate growth: request a Lambda account concurrency increase via Service Quotas. The default reduced limit for new accounts is 10.
4. If abuse: rely on the budget kill switch as the next-tier backstop. Consider adding WAF if the environment doesn't have it yet.

### WAF blocked-requests alarm fired (prod only)

WAF is blocking a meaningful volume of requests — by definition, an attack is in progress and the rate-based rule or common rule set is catching it.

1. Inspect WAF logs in CloudWatch:

   ```bash
   aws logs tail aws-waf-logs-loci-infra-prod-cloudfront \
     --since 30m --region us-east-1
   ```

2. Determine the attack pattern (single IP, distributed, common-rule-set matches).
3. The defense is already working — no urgent action required. If the volume is sustained and you want a tighter response, add an IP-set rule to permanently block specific sources.

## Manual operations

### Out-of-band routing API key rotation

The SSM parameter for the routing API key has `lifecycle.ignore_changes = [value]` so Terraform won't clobber manual rotations.

```bash
# Generate and write a new key
NEW_KEY=$(openssl rand -hex 32)
aws ssm put-parameter \
  --name /loci-infra/<env>/<city>/bike-map/routing-api-key \
  --value "$NEW_KEY" \
  --overwrite \
  --region us-east-2

# Push the new key into the frontend
# Run the Airflow refresh_bike_map_<city> DAG to redeploy config.json with the new key

# Update the CloudFront Function to expect the new key
cd /loci_platform/infra
tofu apply --var-file=<env>.tfvars
```

The CloudFront Function reads from SSM via a Terraform data source, so the `tofu apply` will re-render the function with the new value.

The Lambda also reads the key from SSM at cold start, so existing warm Lambdas will continue accepting the old key until they're recycled (~5–15 minutes of idle, or via deploy).

### Rotating dev/staging basic auth credentials

```bash
# Taint the random_password to force regeneration
cd /loci_platform/infra
tofu taint -var "environment=dev" random_password.non_prod_auth[0]
tofu apply --var-file=dev.tfvars
```

The new credentials will be in SSM at the same path. Browser sessions still cached against the old credentials will start returning 401 on next page load.

### Deleting orphan ACM certificates

After region changes for cert resources (e.g. the us-east-2 → us-east-1 migration done for Issue 4), `tofu state rm` removes the old certs from Terraform tracking but not from AWS:

```bash
# List orphaned certs in the old region
aws acm list-certificates --region us-east-2 \
  --query "CertificateSummaryList[?contains(DomainName, 'routing-api.')].[CertificateArn,DomainName]" \
  --output table

# Delete each (after confirming they're not in use)
for arn in $(aws acm list-certificates --region us-east-2 \
  --query "CertificateSummaryList[?contains(DomainName, 'routing-api.')].CertificateArn" \
  --output text); do
  aws acm delete-certificate --region us-east-2 --certificate-arn "$arn"
done
```

ACM certs are free, so this is housekeeping rather than urgent.

### `tofu state` commands with variable-driven backends

The S3 backend config uses `var.environment` to select state files. State subcommands need this resolved:

```bash
# Either export the variable
export TF_VAR_environment=dev
tofu state list

# Or pass -var inline (note: single dash, before the resource address)
tofu state rm -var "environment=dev" "module.bike_map[\"chicago\"].aws_acm_certificate.api"
```

## Cost expectations

Steady-state monthly cost across all three environments, assuming modest portfolio traffic (50k routing requests/day in prod):

| Component                     | Cost              |
| ----------------------------- | ----------------- |
| Lambda (compute + invocations) | <$1               |
| API Gateway HTTP API (routing) | ~$1.50            |
| API Gateway REST API (logger)  | ~$13              |
| CloudFront                     | ~$5–15            |
| S3 / Route53 / SSM             | <$5               |
| WAF (prod only)                | ~$7               |
| AWS Budgets / SNS / alarms     | $0 (free tier)    |

Total at modest portfolio scale: under **$50/month across all three environments combined**, of which most is the prod route-logger.

## Worst-case bounds when under attack

With the current configuration:

- **Dev/staging**: bounded by the $10 / $5 / $30 budget thresholds. The kill switch fires at $10 actual spend. Maximum possible damage before kill: roughly $10–30 in the lag between cost incurral and Budgets evaluation.
- **Prod**: bounded by the $30 budget kill threshold plus WAF's per-IP rate limiting that makes single-attacker attacks practically impossible to scale. Distributed attacks would hit the budget threshold faster but still cap at roughly the same range.

## Documentation links

- Original issue writeups: `issues/`
- Each module: `infra/modules/<name>/main.tf` is the source of truth for what that module does.
- AWS Budgets in console: <https://us-east-1.console.aws.amazon.com/billing/home#/budgets>
- CloudWatch alarms in console: <https://us-east-2.console.aws.amazon.com/cloudwatch/home?region=us-east-2#alarmsV2:>
- WAF (prod, us-east-1 console): <https://us-east-1.console.aws.amazon.com/wafv2/homev2/web-acls?region=global>
