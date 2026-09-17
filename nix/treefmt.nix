{harbor-rs}: {pkgs, ...}: {
  imports = [
    harbor-rs.inputs.harbor-meta.treefmtModules.nix
    harbor-rs.inputs.harbor-meta.treefmtModules.toml
    harbor-rs.treefmtModules.rust
  ];
  projectRootFile = "flake.nix";

  programs.rustfmt = {
    edition = "2021";
    package = pkgs.rust-bin.stable.latest.default.override {
      extensions = ["rustfmt"];
    };
  };
}
