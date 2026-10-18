{
  config,
  lib,
  pkgs,
  ...
}:
with lib; let
  cfg = config.services.stream-manager;
in {
  options.services.stream-manager = {
    enable = mkEnableOption "stream-manager livestream metadata UI (inactive by default)";
    package = mkOption {
      type = types.package;
      defaultText = "stream-manager flake package";
      description = "stream-manager package to install.";
    };
    databasePath = mkOption {
      type = types.str;
      default = "~/.local/share/stream-manager/app.db";
      description = "Local SQLite path for drafts, bindings and publish history. Exported as STREAM_MANAGER_DB.";
    };
    demoMode = mkOption {
      type = types.bool;
      default = false;
      description = "Launch with the labelled offline demo (fixtures, no credentials). Exported as STREAM_MANAGER_DEMO.";
    };
    tokenStorePath = mkOption {
      type = types.str;
      default = "~/.config/stream-manager/tokens.json";
      description = "Writable AES-256-GCM OAuth token store (never an agenix-managed path). The app ensures its parent directory exists with mode 0700.";
    };
    tokenKeyPath = mkOption {
      type = types.str;
      default = "";
      description = "Operator-provisioned 32-byte runtime key file for the token store (empty = locked state, never plaintext fallback). Only the path belongs here.";
    };
    chat = {
      enable = mkOption {
        type = types.bool;
        default = true;
        description = "Show the Chat workspace tab.";
      };
      autoConnect = mkOption {
        type = types.bool;
        default = false;
        description = "Auto-connect restored chat sources at startup. Default off: connections are explicit operator actions.";
      };
      bufferMessages = mkOption {
        type = types.int;
        default = 5000;
        description = "Live chat buffer: max timeline entries (100-50000).";
      };
      bufferBytes = mkOption {
        type = types.int;
        default = 8388608;
        description = "Live chat buffer: max message bytes.";
      };
      kickRelayUrl = mkOption {
        type = types.str;
        default = "";
        description = "Operator relay base URL for Kick webhooks (empty = Relay required state).";
      };
      kickRelayBearerPath = mkOption {
        type = types.str;
        default = "";
        description = "File holding the relay egress bearer token (path only).";
      };
      rumbleApiUrlPath = mkOption {
        type = types.str;
        default = "";
        description = "File holding the Rumble Live Stream API URL (bearer secret; path only).";
      };
      twitchClientId = mkOption {
        type = types.str;
        default = "";
        description = "Operator Twitch app client id (public identifier, not a secret).";
      };
      youtubeClientId = mkOption {
        type = types.str;
        default = "";
        description = "Operator Google Cloud OAuth client id (public identifier, not a secret).";
      };
      kickClientId = mkOption {
        type = types.str;
        default = "";
        description = "Operator Kick app client id (public identifier, not a secret).";
      };
      kickClientSecretPath = mkOption {
        type = types.str;
        default = "";
        description = "File holding the Kick app client secret (path only; required for Kick token exchange).";
      };
      oauthRedirectUri = mkOption {
        type = types.str;
        default = "http://127.0.0.1:8912/callback";
        description = "Loopback OAuth callback registered in each provider app.";
      };
      serverUrl = mkOption {
        type = types.str;
        default = "";
        description = "Central stream-manager-server base URL (empty = standalone desktop mode). Same-LAN address, e.g. http://192.168.178.30:8921.";
      };
      serverTokenPath = mkOption {
        type = types.str;
        default = "";
        description = "File holding this client's bearer token for the central server (path only; individually revocable per operator).";
      };
    };
    instantcloneStatusCommand = mkOption {
      type = types.str;
      default = "canix --output json secret instantclone status";
      description = "Non-secret InstantClone status source (name/platform/enabled/encryptedFileReadable). Consumed by the app via config file or STREAM_MANAGER_STATUS_CMD.";
    };
  };

  # Deliberately install-only: no services, no activation, no firewall rules.
  config = mkIf cfg.enable {
    home.packages = [cfg.package];
    home.sessionVariables = {
      STREAM_MANAGER_DB = cfg.databasePath;
      STREAM_MANAGER_DEMO = lib.boolToString cfg.demoMode;
    };
    xdg.configFile."stream-manager/config.toml".text = ''
      # Managed by Home Manager (services.stream-manager). Non-secret only:
      # only runtime paths appear here, never secret contents.
      database_path = "${cfg.databasePath}"
      demo_mode = ${lib.boolToString cfg.demoMode}
      token_store_path = "${cfg.tokenStorePath}"
      token_key_path = "${cfg.tokenKeyPath}"
      instantclone_status_command = "${cfg.instantcloneStatusCommand}"
      chat_enabled = ${lib.boolToString cfg.chat.enable}
      chat_auto_connect = ${lib.boolToString cfg.chat.autoConnect}
      chat_buffer_messages = ${toString cfg.chat.bufferMessages}
      chat_buffer_bytes = ${toString cfg.chat.bufferBytes}
      kick_relay_url = "${cfg.chat.kickRelayUrl}"
      kick_relay_bearer_path = "${cfg.chat.kickRelayBearerPath}"
      rumble_api_url_path = "${cfg.chat.rumbleApiUrlPath}"
      twitch_client_id = "${cfg.chat.twitchClientId}"
      youtube_client_id = "${cfg.chat.youtubeClientId}"
      kick_client_id = "${cfg.chat.kickClientId}"
      kick_client_secret_path = "${cfg.chat.kickClientSecretPath}"
      oauth_redirect_uri = "${cfg.chat.oauthRedirectUri}"
      chat_server_url = "${cfg.chat.serverUrl}"
      chat_server_token_path = "${cfg.chat.serverTokenPath}"
    '';
  };
}
