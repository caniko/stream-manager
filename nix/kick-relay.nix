{
  config,
  lib,
  pkgs,
  ...
}:
with lib; let
  cfg = config.services.kick-chat-relay;
in {
  options.services.kick-chat-relay = {
    enable = mkEnableOption "Kick chat webhook relay (disabled by default; never auto-deployed)";
    package = mkOption {
      type = types.package;
      defaultText = "kick-chat-relay flake package";
      description = "kick-chat-relay package to run.";
    };
    listen = mkOption {
      type = types.str;
      default = "127.0.0.1:8913";
      description = "Local bind address. Public ingress must come from the operator's own reverse proxy with TLS; this module opens no firewall ports.";
    };
    publicKeyPath = mkOption {
      type = types.path;
      description = "Kick webhook public key (PEM file). Only the path is referenced.";
    };
    bearerPath = mkOption {
      type = types.path;
      description = "Desktop egress bearer token file. Only the path is referenced.";
    };
    channelsPath = mkOption {
      type = types.path;
      description = "Newline-separated allowed channel logins file. Only the path is referenced.";
    };
  };

  # Deliberately minimal: no firewall rules, no proxy wiring, no Kick
  # subscription registration. The operator points their reverse proxy at
  # `listen` and registers the public URL in the Kick developer portal.
  #
  # Secret-bearing files arrive through LoadCredential under fixed names:
  # the DynamicUser unit never needs direct access to root-owned agenix
  # paths, and no secret permission is weakened to accommodate it.
  config = mkIf cfg.enable {
    assertions = [
      {
        assertion = cfg.publicKeyPath != null && cfg.bearerPath != null && cfg.channelsPath != null;
        message = "services.kick-chat-relay needs publicKeyPath, bearerPath and channelsPath.";
      }
    ];
    systemd.services.kick-chat-relay = {
      description = "Kick chat webhook relay (operator-managed ingress)";
      wantedBy = ["multi-user.target"];
      after = ["network-online.target"];
      serviceConfig = {
        ExecStart = "${cfg.package}/bin/kick-chat-relay --listen ${cfg.listen} --public-key /run/credentials/kick-chat-relay/kick-public-key --bearer /run/credentials/kick-chat-relay/kick-bearer --channels /run/credentials/kick-chat-relay/kick-channels";
        DynamicUser = true;
        LoadCredential = [
          "kick-public-key:${cfg.publicKeyPath}"
          "kick-bearer:${cfg.bearerPath}"
          "kick-channels:${cfg.channelsPath}"
        ];
        NoNewPrivileges = true;
        ProtectSystem = "strict";
        ProtectHome = true;
        PrivateTmp = true;
        Restart = "on-failure";
      };
    };
  };
}
