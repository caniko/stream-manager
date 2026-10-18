{
  description = "stream-manager — livestream metadata preparation and publishing UI (iced)";

  inputs = {
    harbor-rs.url = "github:caniko/harbor-rs/trunk";
    nixpkgs.follows = "harbor-rs/nixpkgs";
    rust-overlay.follows = "harbor-rs/rust-overlay";
    crane.follows = "harbor-rs/crane";
    treefmt-nix.follows = "harbor-rs/treefmt-nix";
    git-hooks.follows = "harbor-rs/git-hooks";
  };

  outputs = {
    self,
    nixpkgs,
    harbor-rs,
    rust-overlay,
    treefmt-nix,
    git-hooks,
    ...
  }: let
    systems = ["x86_64-linux"];
    forSystem = system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [(import rust-overlay)];
      };
      # Stable compiler: iced + rusqlite build on stable; keeps Cargo
      # profiles compatible (no nightly-only flags via channel="stable").
      toolchain = harbor-rs.lib.mkToolchain {
        inherit pkgs;
        toolchainProfile = "stable";
      };
      inherit (toolchain) craneLib rustToolchain;
      cargoConfig = harbor-rs.lib.mkCargoConfig {
        inherit pkgs;
        channel = "stable";
        crossTargets = ["x86_64-unknown-linux-gnu"];
      };
      cross = harbor-rs.lib.mkCross {inherit pkgs system;};
      treefmtEval = treefmt-nix.lib.evalModule pkgs (import ./nix/treefmt.nix {inherit harbor-rs;});
      pre-commit-check = git-hooks.lib.${system}.run {
        src = ./.;
        hooks = import ./nix/pre-commit.nix {
          inherit pkgs;
          treefmtWrapper = treefmtEval.config.build.wrapper;
          inherit rustToolchain;
        };
      };
      # Native GUI runtime libs only. GPU drivers come from the host NixOS
      # (vulkan-loader ICD discovery); no bundled driver stack here.
      guiLibs = with pkgs; [
        wayland
        libxkbcommon
        fontconfig
        freetype
        dejavu_fonts
        sqlite
        vulkan-loader
        libGL
        libX11
        libXcursor
        libXi
        libXrandr
        libxcb
      ];
      src = craneLib.cleanCargoSource ./.;
      commonArgs = {
        inherit src;
        pname = "stream-manager";
        version = "0.1.0";
        strictDeps = true;
        nativeBuildInputs = [pkgs.makeWrapper pkgs.pkg-config];
        buildInputs = guiLibs;
      };
      cargoArtifacts = craneLib.buildDepsOnly commonArgs;
      package = craneLib.buildPackage (commonArgs
        // {
          inherit cargoArtifacts;
          postInstall = ''
            wrapProgram $out/bin/stream-manager \
              --prefix LD_LIBRARY_PATH : "${pkgs.lib.makeLibraryPath guiLibs}" \
              --set FONTCONFIG_FILE "${pkgs.fontconfig.out}/etc/fonts/fonts.conf"
          '';
          meta.mainProgram = "stream-manager";
        });
      # Opt-in Kick webhook relay: packaged, never auto-deployed. The
      # desktop stays fully usable without it (Relay required state).
      relay = craneLib.buildPackage (commonArgs
        // {
          inherit cargoArtifacts;
          pname = "kick-chat-relay";
          version = "0.1.0";
          cargoExtraArgs = "--bin kick-chat-relay";
          meta.mainProgram = "kick-chat-relay";
        });
      # Central service (Nomad): canonical accounts, chat session,
      # provider subscriptions, credentials, and action admission for
      # native clients. Packaged only; enabling is an operator decision
      # in the fleet configuration.
      server = craneLib.buildPackage (commonArgs
        // {
          inherit cargoArtifacts;
          pname = "stream-manager-server";
          version = "0.1.0";
          cargoExtraArgs = "--bin stream-manager-server";
          meta.mainProgram = "stream-manager-server";
        });
    in {
      inherit pkgs toolchain craneLib cargoConfig cross treefmtEval pre-commit-check package relay server cargoArtifacts guiLibs;
    };
  in {
    packages = nixpkgs.lib.genAttrs systems (system: {
      default = (forSystem system).package;
      stream-manager = (forSystem system).package;
      kick-chat-relay = (forSystem system).relay;
      stream-manager-server = (forSystem system).server;
    });

    apps = nixpkgs.lib.genAttrs systems (system: let
      pkg = (forSystem system).package;
    in {
      default = {
        type = "app";
        program = "${pkg}/bin/stream-manager";
      };
    });

    formatter = nixpkgs.lib.genAttrs systems (
      system: (forSystem system).treefmtEval.config.build.wrapper
    );

    checks = nixpkgs.lib.genAttrs systems (system: let
      cfg = forSystem system;
    in {
      default = cfg.package;
      formatting = cfg.treefmtEval.config.build.check self;
      clippy = cfg.craneLib.cargoClippy {
        src = cfg.craneLib.cleanCargoSource ./.;
        pname = "stream-manager-clippy";
        version = "0.1.0";
        strictDeps = true;
        cargoArtifacts = cfg.cargoArtifacts;
        cargoClippyExtraArgs = "--all-targets -- --deny warnings";
        nativeBuildInputs = [cfg.pkgs.pkg-config];
        buildInputs = cfg.guiLibs;
      };
      # Enabled-module evaluation: the server module must evaluate with
      # credentials wired as single LoadCredential entries. The generated
      # JSON references the same `credPath` helper, so name correspondence
      # holds by construction (reading the store JSON back here would be
      # an import-from-derivation, forbidden by the check invocation).
      server-module-eval = let
        evaluated = nixpkgs.lib.evalModules {
          specialArgs = {pkgs = cfg.pkgs;};
          modules = [
            # Provides the top-level `assertions` option outside NixOS.
            "${nixpkgs}/nixos/modules/misc/assertions.nix"
            # Minimal systemd.services shape for this test (a full
            # nixosSystem evaluation is fleet-integration work, not a
            # unit check). LoadCredential stays a list of strings, so
            # malformed entries fail evaluation here.
            ({lib, ...}: {
              options.systemd.services = lib.mkOption {
                type = lib.types.attrsOf (lib.types.submodule {
                  options = {
                    description = lib.mkOption {
                      type = lib.types.str;
                      default = "";
                    };
                    wantedBy = lib.mkOption {
                      type = lib.types.listOf lib.types.str;
                      default = [];
                    };
                    after = lib.mkOption {
                      type = lib.types.listOf lib.types.str;
                      default = [];
                    };
                    serviceConfig = lib.mkOption {
                      type = lib.types.attrsOf lib.types.unspecified;
                      default = {};
                    };
                  };
                });
                default = {};
              };
            })
            self.nixosModules.stream-manager-server
            ({...}: {
              services.stream-manager-server = {
                enable = true;
                package = cfg.pkgs.hello;
                operators = [
                  {
                    id = "can";
                    role = "admin";
                    tokenSha256 = "abc";
                    channels = [];
                  }
                ];
                tokenKeyFile = "/run/agenix/test-key";
                rumbleApiUrlFile = "/run/agenix/test-rumble";
              };
            })
          ];
        };
        unit = evaluated.config.systemd.services.stream-manager-server.serviceConfig;
      in
        assert unit.LoadCredential
        == [
          "sm-token-key:/run/agenix/test-key"
          "sm-rumble-api-url:/run/agenix/test-rumble"
        ];
        assert unit.DynamicUser == true;
        assert unit.StateDirectory == "stream-manager-server";
        assert nixpkgs.lib.hasPrefix "${cfg.pkgs.hello}/bin/stream-manager-server --config "
        unit.ExecStart;
        assert evaluated.config.services.stream-manager-server.databasePath
        == "/var/lib/stream-manager-server/app.db";
          cfg.pkgs.writeText "server-module-eval-ok" "ok";
    });

    devShells = nixpkgs.lib.genAttrs systems (system: let
      cfg = forSystem system;
    in
      harbor-rs.lib.mkDevShells {
        inherit (cfg) pkgs cross cargoConfig;
        inherit (cfg.toolchain) craneLib;
        packages =
          cfg.pre-commit-check.enabledPackages
          ++ cfg.guiLibs
          ++ (with cfg.pkgs; [pkg-config sqlite ffmpeg vulkan-tools]);
        extraShellHook = cfg.pre-commit-check.shellHook;
      });

    homeModules = {
      stream-manager = import ./nix/home-manager.nix;
      default = self.homeModules.stream-manager;
    };

    nixosModules = {
      kick-chat-relay = import ./nix/kick-relay.nix;
      stream-manager-server = import ./nix/server-module.nix;
      default = self.nixosModules.stream-manager-server;
    };
  };
}
