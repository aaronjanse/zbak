{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/832ae4311a44ddfee300d54e17268f448b8ea8ea";
    flake-utils.url = "github:numtide/flake-utils";
  };


  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let pkgs = nixpkgs.legacyPackages.${system}; in
      rec {
        packages = flake-utils.lib.flattenTree {
          zbak = pkgs.callPackage
            (
              { lib, makeWrapper, openssh, rustPlatform, zfs }:
              rustPlatform.buildRustPackage rec {
                name = "zbak";
                src = ./.;
                nativeBuildInputs = [ makeWrapper ];
                fixupPhase = ''
                  wrapProgram $out/bin/zbak \
                    --set PATH ${lib.makeBinPath [ zfs openssh ]}
                '';
                cargoSha256 = "sha256-OgRGWOZin8d/HTbxzJ7vxpTmQHhfs5RlJwhcJPaiMB0=";
              }
            )
            { };
        };
        defaultPackage = packages.zbak;
      }
    );
}
