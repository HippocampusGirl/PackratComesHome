{
  description = "A basic flake with a shell";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };
        pythonPackages = with pkgs.python311Packages; [
          dropbox
          more-itertools
          pip
          pyyaml
          sqlalchemy
          tqdm
          types-pyyaml
        ];
      in {
        devShells.default = pkgs.mkShell {
          buildInputs = with pkgs; [ python311 ] ++ pythonPackages;
        };
      });
}
