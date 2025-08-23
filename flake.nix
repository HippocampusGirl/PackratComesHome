{
  description = "A basic flake with a shell";
  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixpkgs-unstable";
    flake-utils.url = "github:numtide/flake-utils";
    pyproject.url = "github:nix-community/pyproject.nix";
  };

  outputs = { self, nixpkgs, flake-utils, pyproject }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        inherit (nixpkgs) lib;

        pkgs = import nixpkgs {
          inherit system;
          config.allowUnfree = true;
        };

        python = pkgs.python313.withPackages (py:
          with py; [
            dropbox
            more-itertools
            mypy
            pip
            pyyaml
            requests
            sqlalchemy
            tqdm
            types-requests
            types-pyyaml
            types-tqdm
          ]);

        # Loads pyproject.toml into a high-level project representation
        project = pyproject.lib.project.loadPyproject {
          pyproject = lib.importTOML ./pyproject.toml;
        };
        objectReferenceToModule = objectReference:
          builtins.elemAt (lib.splitString ":" objectReference) 0;
        objectReferenceToAttr = objectReference:
          builtins.elemAt (lib.splitString ":" objectReference) 1;
        scripts = (lib.mapAttrsToList (name: objectReference:
          (pkgs.writeScriptBin name ''
            #!${python.interpreter}
            # -*- coding: utf-8 -*-
            import re
            import sys
            from ${objectReferenceToModule objectReference} import ${
              objectReferenceToAttr objectReference
            }
            if __name__ == "__main__":
                sys.argv[0] = re.sub(r"(-script\.pyw|\.exe)?$", "", sys.argv[0])
                sys.exit(${objectReferenceToAttr objectReference}())
          '')) project.pyproject.project.scripts);

      in {
        devShells.default = pkgs.mkShell {
          buildInputs = [ python ] ++ scripts;
          shellHook = ''
            PYTHONPATH=$(git rev-parse --show-toplevel)/src:$PYTHONPATH
            export PYTHONPATH
          '';
        };
      });
}
