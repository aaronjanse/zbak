{
  inputs.nixpkgs.url = "github:nixos/nixpkgs/832ae4311a44ddfee300d54e17268f448b8ea8ea";

  outputs = { self, nixpkgs }: {
    defaultPackage.x86_64-linux = self.packages.x86_64-linux.zbak;
    packages.x86_64-linux.zbak = nixpkgs.legacyPackages.x86_64-linux.callPackage (
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
    ) { };
  };
}
