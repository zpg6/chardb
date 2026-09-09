use std::{env, time::Duration};

use chardb::{Client, ClientConfig, Mutation, Query, SubscriptionEvent};
use serde::{Deserialize, Serialize};

#[derive(Serialize)]
struct QueryArgs {
    #[serde(rename = "organizationId")]
    organization_id: String,
}

#[derive(Serialize)]
struct MutationArgs {
    id: String,
    #[serde(rename = "organizationId")]
    organization_id: String,
    body: String,
    #[serde(rename = "createdAt")]
    created_at: u64,
}

#[derive(Deserialize)]
struct MutationAck {
    id: String,
    #[serde(rename = "userId")]
    user_id: String,
    #[serde(rename = "tenantId")]
    tenant_id: String,
    role: String,
    roles: Vec<String>,
}

const LIST_ORGANIZATION_ROWS: Query<QueryArgs, serde_json::Value> =
    Query::new("test/workerd/gateway-jwt.entry.ts#listOrganizationRows");
const WRITE_ORGANIZATION_ROW: Mutation<MutationArgs, MutationAck> =
    Mutation::new("test/workerd/gateway-jwt.entry.ts#writeOrganizationRow");

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args().skip(1);
    let endpoint = arguments.next().ok_or("missing endpoint")?;
    let token = arguments.next().ok_or("missing JWT")?;
    let row_id = arguments.next().ok_or("missing row id")?;
    if arguments.next().is_some() {
        return Err("unexpected extra argument".into());
    }

    let client = Client::connect(
        ClientConfig::with_token(endpoint, token)
            .connect_timeout(Duration::from_secs(5))
            .welcome_timeout(Duration::from_secs(5)),
    )?;
    let mut subscription = client.subscribe(
        LIST_ORGANIZATION_ROWS,
        &QueryArgs {
            organization_id: "workerd-org".to_owned(),
        },
    )?;
    match subscription.recv_timeout(Duration::from_secs(5))? {
        Some(SubscriptionEvent::Snapshot { .. }) => {}
        Some(event) => return Err(format!("expected snapshot, got {event:?}").into()),
        None => return Err("timed out waiting for snapshot".into()),
    }

    let result: MutationAck = client.mutate_with_id(
        WRITE_ORGANIZATION_ROW,
        &MutationArgs {
            id: row_id.clone(),
            organization_id: "workerd-org".to_owned(),
            body: "rust-workerd-conformance".to_owned(),
            created_at: 1,
        },
        format!("rust-{row_id}"),
    )?;
    if result.id != row_id {
        return Err(format!("mutation returned unexpected id {}", result.id).into());
    }
    if result.user_id != "workerd-user"
        || result.tenant_id != "workerd-org"
        || result.role != "member"
        || result.roles != ["member"]
    {
        return Err("mutation did not preserve verified organization and user authority".into());
    }
    match subscription.recv_timeout(Duration::from_secs(5))? {
        Some(SubscriptionEvent::Update { rows } | SubscriptionEvent::Snapshot { rows })
            if rows.iter().any(|row| {
                row.get("id").and_then(serde_json::Value::as_str) == Some(&row_id)
                    && row
                        .get("organizationId")
                        .and_then(serde_json::Value::as_str)
                        == Some("workerd-org")
                    && row.get("authorId").and_then(serde_json::Value::as_str)
                        == Some("workerd-user")
            }) => {}
        Some(event) => {
            return Err(format!("expected canonical rows for {row_id}, got {event:?}").into())
        }
        None => return Err("timed out waiting for live update".into()),
    }
    client.close();
    println!(
        "{{\"ok\":true,\"rowId\":{}}}",
        serde_json::to_string(&row_id)?
    );
    Ok(())
}
