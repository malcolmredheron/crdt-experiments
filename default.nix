# nixos-unstable. Find the current hash at https://status.nixos.org/.
# Check https://nodejs.org/en/about/releases/ for the current npm release.
{ nixpkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/ff8e8d4b4f7c934e16546beefce9e547b6b65d5c.tar.gz") {config.allowUnfree = true;}
}:

let
  pkgs = [
    nixpkgs.docker
    nixpkgs.docker-compose
    nixpkgs.git
    nixpkgs.nodejs-18_x
    nixpkgs.openssh
    nixpkgs.which
  ];

in
  nixpkgs.stdenv.mkDerivation {
    name = "crdt-experiments";
    buildInputs = pkgs;
  }
