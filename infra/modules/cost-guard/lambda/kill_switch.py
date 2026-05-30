"""Kill switch — disables API Gateway stages on cost overrun.

Subscribes to the cost-guard kill SNS topic. When invoked, sets throttle
limits to 0 on every API Gateway stage passed in via HTTP_API_STAGES and
REST_API_STAGES env vars, then publishes a confirmation to the alerts SNS
topic.

To restore service, re-run `tofu apply` — Terraform will detect the
drifted throttle limits and put them back.
"""

import json
import os
import boto3


def lambda_handler(event, context):
    apigw_v2 = boto3.client("apigatewayv2")
    apigw_v1 = boto3.client("apigateway")
    sns = boto3.client("sns")

    http_stages = json.loads(os.environ["HTTP_API_STAGES"])
    rest_stages = json.loads(os.environ["REST_API_STAGES"])
    alert_topic_arn = os.environ["ALERT_SNS_TOPIC_ARN"]

    disabled = []
    errors = []

    for s in http_stages:
        label = f"http:{s['api_id']}/{s['stage_name']}"
        try:
            apigw_v2.update_stage(
                ApiId=s["api_id"],
                StageName=s["stage_name"],
                DefaultRouteSettings={
                    "ThrottlingBurstLimit": 0,
                    "ThrottlingRateLimit": 0.0,
                },
            )
            disabled.append(label)
        except Exception as e:
            errors.append(f"{label}: {e}")

    for s in rest_stages:
        label = f"rest:{s['rest_api_id']}/{s['stage_name']}"
        try:
            apigw_v1.update_stage(
                restApiId=s["rest_api_id"],
                stageName=s["stage_name"],
                patchOperations=[
                    {"op": "replace", "path": "/*/*/throttling/rateLimit", "value": "0"},
                    {"op": "replace", "path": "/*/*/throttling/burstLimit", "value": "0"},
                ],
            )
            disabled.append(label)
        except Exception as e:
            errors.append(f"{label}: {e}")

    summary = {
        "disabled": disabled,
        "errors": errors,
        "triggering_event": event,
    }

    sns.publish(
        TopicArn=alert_topic_arn,
        Subject="Cost guard kill switch fired",
        Message=(
            f"Cost guard kill switch fired.\n\n"
            f"Disabled stages: {len(disabled)}\n"
            f"Errors: {len(errors)}\n\n"
            f"Details:\n{json.dumps(summary, indent=2, default=str)}\n\n"
            f"To restore service, run `tofu apply` to reset throttle limits."
        ),
    )

    return summary
