{
  description = "watchable-rs: Observable values with change tracking";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    rust-overlay.url = "github:oxalica/rust-overlay";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (system: let
      pkgs = import nixpkgs {
        inherit system;
        overlays = [rust-overlay.overlays.default];
      };
    in {
      devShells.default = pkgs.mkShell {
        packages = with pkgs; [
          (rust-bin.stable.latest.default.override {
            extensions = ["rust-src" "rust-analyzer"];
          })
        ];

        RUST_BACKTRACE = 1;
      };
    });
}
