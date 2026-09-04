//! Browser authorization for native CLIs. Enable the `browser-login` feature.
//!
//! The application supplies an authorization endpoint that supports public clients,
//! S256 PKCE, and `http://127.0.0.1:{port}/callback` redirects with ephemeral ports.
//! A Better Auth browser sign-in page alone does not implement this contract.

use std::{
    fmt,
    io::{Read, Write},
    net::{TcpListener, TcpStream},
    thread,
    time::{Duration, Instant},
};

use oauth2::{url::Url, CsrfToken, PkceCodeChallenge};
pub use oauth2::{AuthorizationCode, PkceCodeVerifier, RedirectUrl};

use crate::{Error, ErrorKind, Result};

/// A single browser authorization attempt. Dropping it closes the listener.
/// Debug output omits the URL, state, and PKCE verifier.
pub struct BrowserLogin {
    listener: TcpListener,
    authorization_url: Url,
    state: CsrfToken,
    verifier: PkceCodeVerifier,
    redirect_uri: RedirectUrl,
    deadline: Instant,
}

impl fmt::Debug for BrowserLogin {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("BrowserLogin").finish_non_exhaustive()
    }
}

/// A validated callback, ready for the application's token exchange.
/// Send these values only to the configured authorization server's token endpoint.
#[derive(Debug)]
pub struct BrowserAuthorization {
    pub code: AuthorizationCode,
    pub pkce_verifier: PkceCodeVerifier,
    pub redirect_uri: RedirectUrl,
}

impl BrowserAuthorization {
    /// Exchange the code using the caller's HTTP client, always including PKCE.
    /// Configure that client with a timeout and redirect following disabled.
    /// The access token is usable with `ClientConfig` only if the authorization
    /// server issues JWTs with the issuer, audience, and claims the Gateway expects.
    ///
    /// # Errors
    /// Returns an error for an invalid endpoint or a failed token exchange.
    pub fn exchange<C: oauth2::SyncHttpClient>(
        self,
        client_id: &str,
        token_url: &str,
        http_client: &C,
    ) -> Result<oauth2::basic::BasicTokenResponse> {
        let url = Url::parse(token_url).map_err(|_| configuration())?;
        if !secure_endpoint(&url) || client_id.is_empty() {
            return Err(configuration());
        }
        oauth2::basic::BasicClient::new(oauth2::ClientId::new(client_id.to_owned()))
            .set_token_uri(oauth2::TokenUrl::from_url(url))
            .set_redirect_uri(self.redirect_uri)
            .exchange_code(self.code)
            .set_pkce_verifier(self.pkce_verifier)
            .request(http_client)
            .map_err(|_| {
                Error::local(
                    ErrorKind::Authentication,
                    "browser login token exchange failed",
                )
            })
    }
}

impl BrowserLogin {
    /// Bind a temporary IPv4 loopback listener and construct an authorization URL.
    ///
    /// Supply `client_id`, scopes, and any provider-specific parameters in `url`.
    /// This method owns `response_type`, `redirect_uri`, `state`, and PKCE parameters.
    /// The deadline starts here and includes time spent in the browser.
    ///
    /// # Errors
    /// Returns an error for an invalid URL, timeout, or listener bind failure.
    pub fn start(url: &str, timeout: Duration) -> Result<Self> {
        let mut url = Url::parse(url).map_err(|_| configuration())?;
        if !secure_endpoint(&url) || timeout.is_zero() {
            return Err(configuration());
        }
        let mut clients = 0;
        for (key, value) in url.query_pairs() {
            if matches!(
                key.as_ref(),
                "request"
                    | "request_uri"
                    | "response_type"
                    | "response_mode"
                    | "redirect_uri"
                    | "state"
                    | "code_challenge"
                    | "code_challenge_method"
            ) {
                return Err(configuration());
            }
            if key == "client_id" {
                if value.is_empty() {
                    return Err(configuration());
                }
                clients += 1;
            }
        }
        if clients != 1 {
            return Err(configuration());
        }
        let deadline = Instant::now()
            .checked_add(timeout)
            .ok_or_else(configuration)?;
        let listener = TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, 0)).map_err(transport)?;
        listener.set_nonblocking(true).map_err(transport)?;
        let redirect_uri = RedirectUrl::new(format!(
            "http://{}/callback",
            listener.local_addr().map_err(transport)?
        ))
        .map_err(|_| configuration())?;
        let state = CsrfToken::new_random();
        let (challenge, verifier) = PkceCodeChallenge::new_random_sha256();
        url.query_pairs_mut()
            .append_pair("response_type", "code")
            .append_pair("redirect_uri", redirect_uri.as_str())
            .append_pair("state", state.secret())
            .append_pair("code_challenge", challenge.as_str())
            .append_pair("code_challenge_method", "S256");
        Ok(Self {
            listener,
            authorization_url: url,
            state,
            verifier,
            redirect_uri,
            deadline,
        })
    }

    /// URL to display when a browser cannot be launched automatically.
    #[must_use]
    pub fn authorization_url(&self) -> &str {
        self.authorization_url.as_str()
    }

    /// Open the authorization URL in the system browser.
    ///
    /// # Errors
    /// Returns an error if the platform cannot launch a browser or time has expired.
    /// The attempt remains usable, so the CLI can display `authorization_url` instead.
    pub fn open_browser(&self) -> Result<()> {
        remaining(self.deadline)?;
        webbrowser::open(self.authorization_url())
            .map_err(|_| Error::local(ErrorKind::Transport, "could not open the login browser"))
    }

    /// Wait for one valid callback. Invalid requests do not consume the attempt.
    ///
    /// This blocks the calling thread. The listener closes on every return path.
    /// Completion means a code was received, not that the user is authenticated.
    /// The authorization server must verify PKCE during the token exchange.
    ///
    /// # Errors
    /// Returns an error on timeout, authorization denial, or listener failure.
    pub fn wait(self) -> Result<BrowserAuthorization> {
        loop {
            let left = remaining(self.deadline)?;
            match self.listener.accept() {
                Ok((mut stream, _)) => {
                    stream.set_nonblocking(false).map_err(transport)?;
                    let request_deadline =
                        self.deadline.min(Instant::now() + Duration::from_secs(1));
                    let callback = self.read_callback(&mut stream, request_deadline);
                    let (status, body) = match &callback {
                        Some(Ok(_)) => (
                            "200 OK",
                            "Authorization received. Return to your terminal to finish signing in.",
                        ),
                        Some(Err(_)) => (
                            "400 Bad Request",
                            "Authorization was denied. Return to your terminal.",
                        ),
                        None => ("400 Bad Request", "Invalid login callback."),
                    };
                    let _ = stream.set_write_timeout(Some(Duration::from_millis(100).min(left)));
                    let response = format!("HTTP/1.1 {status}\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: {}\r\nCache-Control: no-store\r\nReferrer-Policy: no-referrer\r\nContent-Security-Policy: default-src 'none'; frame-ancestors 'none'\r\nConnection: close\r\n\r\n{body}", body.len());
                    let _ = stream.write_all(response.as_bytes());
                    if let Some(code) = callback {
                        remaining(self.deadline)?;
                        return Ok(BrowserAuthorization {
                            code: code?,
                            pkce_verifier: self.verifier,
                            redirect_uri: self.redirect_uri,
                        });
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(left.min(Duration::from_millis(10)));
                }
                Err(error) => return Err(transport(error)),
            }
        }
    }

    fn read_callback(
        &self,
        stream: &mut TcpStream,
        deadline: Instant,
    ) -> Option<Result<AuthorizationCode>> {
        let mut bytes = Vec::new();
        let mut chunk = [0; 1024];
        while bytes.len() < 8192 {
            stream
                .set_read_timeout(Some(remaining(deadline).ok()?))
                .ok()?;
            let count = stream.read(&mut chunk).ok()?;
            if count == 0 {
                return None;
            }
            bytes.extend_from_slice(&chunk[..count]);
            if bytes.len() > 8192 {
                return None;
            }
            if bytes.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }
        let mut headers = [httparse::EMPTY_HEADER; 32];
        let mut request = httparse::Request::new(&mut headers);
        if !request.parse(&bytes).ok()?.is_complete() || request.method != Some("GET") {
            return None;
        }
        let host = self.listener.local_addr().ok()?.to_string();
        let hosts: Vec<_> = request
            .headers
            .iter()
            .filter(|h| h.name.eq_ignore_ascii_case("host"))
            .collect();
        if hosts.len() != 1 || hosts[0].value != host.as_bytes() {
            return None;
        }
        let target = request.path?;
        if !target.starts_with("/callback?") {
            return None;
        }
        let url = Url::parse(&format!("http://{host}{target}")).ok()?;
        if url.path() != "/callback" || url.fragment().is_some() {
            return None;
        }
        let mut state = None;
        let mut code = None;
        let mut error = None;
        for (key, value) in url.query_pairs() {
            let slot = match key.as_ref() {
                "state" => &mut state,
                "code" => &mut code,
                "error" => &mut error,
                _ => continue,
            };
            if slot.replace(value.into_owned()).is_some() {
                return None;
            }
        }
        if CsrfToken::new(state?) != self.state {
            return None;
        }
        match (code, error) {
            (Some(code), None) if !code.is_empty() => Some(Ok(AuthorizationCode::new(code))),
            (None, Some(error)) if !error.is_empty() => Some(Err(Error::local(
                ErrorKind::Authentication,
                "browser authorization was denied",
            ))),
            _ => None,
        }
    }
}

fn remaining(deadline: Instant) -> Result<Duration> {
    deadline
        .checked_duration_since(Instant::now())
        .filter(|d| !d.is_zero())
        .ok_or_else(|| Error::local(ErrorKind::Timeout, "browser login timed out"))
}

fn configuration() -> Error {
    Error::local(ErrorKind::Configuration, "browser login requires HTTPS, one client_id, no reserved authorization parameters, and a positive timeout; HTTP is allowed only for loopback IPs")
}

fn transport(_: std::io::Error) -> Error {
    Error::local(ErrorKind::Transport, "browser login listener failed")
}

fn secure_endpoint(url: &Url) -> bool {
    let loopback = matches!(url.host_str(), Some("127.0.0.1" | "[::1]"));
    (url.scheme() == "https" || (url.scheme() == "http" && loopback))
        && url.host_str().is_some()
        && url.username().is_empty()
        && url.password().is_none()
        && url.fragment().is_none()
}
