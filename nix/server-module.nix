{ config, lib, pkgs, ... }:
with lib; let
  cfg = config.services.stream-manager-server;
  svc = "stream-manager-server";
  cred = name: "sm-${name}";
  # systemd exposes LoadCredential files under
  # /run/credentials/<unit>/, where the unit name keeps its `.service`
  # suffix. Hardcoding the unsuffixed path breaks reads at runtime.
  credPath = name: "/run/credentials/${svc}.service/${cred name}";
  serverJson = pkgs.writeText "stream-manager-server.json" (builtins.toJSON {
    instance = cfg.instance;
    listen = cfg.listen;
    database_path = cfg.databasePath;
    # Mutable state lives in the service StateDirectory; only non-secret
    # settings and token *hashes* land in the store. Secret contents
    # arrive exclusively through LoadCredential paths below.
    token_store_path = "/var/lib/${svc}/tokens.json";
    token_key_path =
      if cfg.tokenKeyFile != null
      then credPath "token-key"
      else "";
    client_ids = cfg.clientIds;
    kick_secret_path =
      if cfg.kickClientSecretFile != null
      then credPath "kick-secret"
      else "";
    kick_relay_url = cfg.kickRelayUrl;
    kick_relay_bearer_path =
      if cfg.kickRelayBearerFile != null
      then credPath "kick-relay-bearer"
      else "";
    rumble_api_path =
      if cfg.rumbleApiUrlFile != null
      then credPath "rumble-api-url"
      else "";
    operators = map (o: {
      id = o.id;
      role = o.role;
      token_sha256 = o.tokenSha256;
      channels = map (c: { provider = c.provider; channel = c.channel; }) o.channels;
    }) cfg.operators;
    accounts = map (a: {
      id = a.id;
      provider = a.provider;
      channel_id = a.channelId;
      channel_name = a.channelName;
    }) cfg.accounts;
  });
  mkCred = name: path: "${cred name}:${path}";
in {
  options.services.stream-manager-server = {
    enable = mkEnableOption "central stream-manager service (disabled by default; never auto-deployed)";
    package = mkOption {
      type = types.package;
      defaultText = "stream-manager-server flake package";
      description = "stream-manager-server package to run.";
    };
    instance = mkOption {
      type = types.str;
      default = "nomad";
      description = "Instance identity reported to clients and receipts.";
    };
    listen = mkOption {
      type = types.str;
      default = "127.0.0.1:8921";
      description = "Private bind address. Public ingress must come from the operator's own reverse proxy; this module opens no firewall ports and registers no subscriptions.";
    };
    operators = mkOption {
      type = types.listOf (types.submodule {
        options = {
          id = mkOption { type = types.str; description = "Operator id (e.g. can, dejana)."; };
          role = mkOption {
            type = types.enum ["admin" "operator"];
            description = "admin manages accounts; operator acts on assigned channels only.";
          };
          tokenSha256 = mkOption {
            type = types.str;
            description = "SHA-256 hex of the client bearer token (hash only, never the token). Generate with: stream-manager-server --gen-token";
          };
          channels = mkOption {
            type = types.listOf (types.submodule {
              options = {
                provider = mkOption { type = types.str; };
                channel = mkOption { type = types.str; };
              };
            });
            default = [];
            description = "Assigned channels for the operator role. Admins need none.";
          };
        };
      });
      default = [];
      description = "Operators allowed to authenticate. Empty refuses to start.";
    };
    accounts = mkOption {
      type = types.listOf (types.submodule {
        options = {
          id = mkOption { type = types.str; };
          provider = mkOption { type = types.str; };
          channelId = mkOption { type = types.str; };
          channelName = mkOption { type = types.str; };
        };
      });
      default = [];
      description = "Seed accounts written once into an empty database. The database is canonical afterwards; manage via UpsertAccount/RemoveAccount.";
    };
    databasePath = mkOption {
      type = types.str;
      default = "/var/lib/stream-manager-server/app.db";
      description = "Canonical SQLite database: account registry and durable action receipts. Must live in the service StateDirectory.";
    };

    clientIds = mkOption {
      type = types.attrsOf types.str;
      default = {};
      description = "Per-provider OAuth client ids keyed by provider slug (public identifiers, not secrets).";
    };
    kickRelayUrl = mkOption {
      type = types.str;
      default = "";
      description = "Operator relay base URL (empty = Relay required state).";
    };
    tokenKeyFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "32-byte runtime key file for the encrypted token store. Without it the credential service reports locked.";
    };
    kickClientSecretFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Kick app client secret file (agenix path; never store contents).";
    };
    kickRelayBearerFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Relay egress bearer token file.";
    };
    rumbleApiUrlFile = mkOption {
      type = types.nullOr types.path;
      default = null;
      description = "Rumble Live Stream API URL file (bearer secret).";
    };
  };

  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.operators != [];
        message = "services.stream-manager-server refuses to start without operators.";
      }
    ];
    systemd.services.stream-manager-server = {
      description = "Central stream-manager service (Nomad-owned chat/account/action state)";
      wantedBy = ["multi-user.target"];
      after = ["network-online.target"];
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/stream-manager-server --config ${serverJson}";
        DynamicUser = true;
        StateDirectory = "stream-manager-server";
        LoadCredential =
          optionals (cfg.tokenKeyFile != null) [(mkCred "token-key" cfg.tokenKeyFile)]
          ++ optionals (cfg.kickClientSecretFile != null) [(mkCred "kick-secret" cfg.kickClientSecretFile)]
          ++ optionals (cfg.kickRelayBearerFile != null) [(mkCred "kick-relay-bearer" cfg.kickRelayBearerFile)]
          ++ optionals (cfg.rumbleApiUrlFile != null) [(mkCred "rumble-api-url" cfg.rumbleApiUrlFile)];
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        Restart = "on-failure";
        RestartSec = "5s";
      };
    };
  };
}
