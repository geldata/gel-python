{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/25.05";
    flake-utils.url = "github:numtide/flake-utils";
    edgedb = {
      url = "github:edgedb/packages-nix";
      inputs.nixpkgs.follows = "nixpkgs";
    };
  };

  outputs =
    {
      self,
      nixpkgs,
      flake-utils,
      edgedb,
    }:
    flake-utils.lib.eachDefaultSystem (
      system:
      let
        pkgs = nixpkgs.legacyPackages.${system};
        gel_pkgs = edgedb.packages.${system};
      in
      {
        devShells.default = pkgs.mkShell {
          venvDir = "./.venv";

          buildInputs = with pkgs; [
            python312Packages.python
            python312Packages.venvShellHook
            python312Packages.python-lsp-server
            python312Packages.python-lsp-ruff
            uv
            ruff

            typos-lsp

            # for gel-python model tests
            gel_pkgs.gel-server-nightly
          ];
        };
      }
    );
}
