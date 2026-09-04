use std::{env, time::Duration};

use chardb_client::{browser_login::BrowserLogin, Client, ClientConfig};
use oauth2::{ureq, TokenResponse};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client_id = env::var("CHARDB_OAUTH_CLIENT_ID")?;
    let mut authorize = oauth2::url::Url::parse(&env::var("CHARDB_AUTHORIZE_URL")?)?;
    authorize
        .query_pairs_mut()
        .append_pair("client_id", &client_id);
    let login = BrowserLogin::start(authorize.as_str(), Duration::from_secs(180))?;
    if login.open_browser().is_err() {
        eprintln!(
            "Open this URL on this computer: {}",
            login.authorization_url()
        );
    }
    let http = ureq::AgentBuilder::new()
        .redirects(0)
        .timeout(Duration::from_secs(15))
        .build();
    let tokens = login
        .wait()?
        .exchange(&client_id, &env::var("CHARDB_TOKEN_URL")?, &http)?;
    let client = Client::connect(ClientConfig::with_token(
        env::var("CHARDB_WS_URL")?,
        tokens.access_token().secret().clone(),
    ))?;
    println!("Authenticated with CharDB.");
    client.close();
    Ok(())
}
