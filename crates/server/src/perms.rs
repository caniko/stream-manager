//! Operator permissions: Can administers, Dejana operates.
//!
//! Every mutating command binds to an authenticated operator identity,
//! never to a machine name. Administrators manage accounts, clients, and
//! assignments; operators read and act only on explicitly assigned
//! channels. Client bearer tokens are stored as SHA-256 hex and compared
//! without branching on content.

use serde::{Deserialize, Serialize};
use stream_manager_chat::model::Provider;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Admin,
    Operator,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChannelScope {
    pub provider: String,
    pub channel: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Operator {
    pub id: String,
    pub role: Role,
    /// SHA-256 hex of the client bearer token. Secrets live in files;
    /// only the hash appears in configuration.
    pub token_sha256: String,
    pub channels: Vec<ChannelScope>,
}

fn subtle_eq(a: &str, b: &str) -> bool {
    let (x, y) = (a.as_bytes(), b.as_bytes());
    if x.len() != y.len() {
        return false;
    }
    x.iter()
        .zip(y.iter())
        .fold(0u8, |acc, (p, q)| acc | (p ^ q))
        == 0
}

pub fn token_hash_hex(token: &str) -> String {
    use sha2::{Digest, Sha256};
    format!("{:x}", Sha256::digest(token.as_bytes()))
}

impl Operator {
    pub fn authenticates(&self, bearer: &str) -> bool {
        subtle_eq(&self.token_sha256, &token_hash_hex(bearer))
    }

    pub fn manages_accounts(&self) -> bool {
        self.role == Role::Admin
    }

    fn assigned(&self, provider: Provider, channel: &str) -> bool {
        self.channels.iter().any(|c| {
            c.provider.eq_ignore_ascii_case(provider.slug())
                && c.channel.eq_ignore_ascii_case(channel)
        })
    }

    pub fn can_read(&self, provider: Provider, channel: &str) -> bool {
        self.role == Role::Admin || self.assigned(provider, channel)
    }

    pub fn can_send(&self, provider: Provider, channel: &str) -> bool {
        self.role == Role::Admin || self.assigned(provider, channel)
    }

    pub fn can_moderate(&self, provider: Provider, channel: &str) -> bool {
        self.role == Role::Admin || self.assigned(provider, channel)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dejana() -> Operator {
        Operator {
            id: "dejana".into(),
            role: Role::Operator,
            token_sha256: token_hash_hex("sekret"),
            channels: vec![ChannelScope {
                provider: "twitch".into(),
                channel: "alice".into(),
            }],
        }
    }

    #[test]
    fn operator_is_channel_scoped() {
        let d = dejana();
        assert!(d.authenticates("sekret"));
        assert!(!d.authenticates("wrong"));
        assert!(d.can_send(Provider::Twitch, "alice"));
        assert!(d.can_send(Provider::Twitch, "ALICE"));
        assert!(!d.can_send(Provider::Twitch, "mallory"));
        assert!(!d.can_send(Provider::Kick, "alice"));
        assert!(!d.manages_accounts());
    }

    #[test]
    fn admin_is_unscoped_but_explicit() {
        let can = Operator {
            id: "can".into(),
            role: Role::Admin,
            token_sha256: token_hash_hex("x"),
            channels: vec![],
        };
        assert!(can.manages_accounts());
        assert!(can.can_moderate(Provider::Rumble, "anyone"));
    }
}
