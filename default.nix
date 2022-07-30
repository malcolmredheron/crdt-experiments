# nixos-unstable. Find the current hash at https://status.nixos.org/.
# Check https://nodejs.org/en/about/releases/ for the current npm release.
{ nixpkgs ? import (fetchTarball "https://github.com/NixOS/nixpkgs/archive/2a93ea177c3d7700b934bf95adfe00c435f696b8.tar.gz") {config.allowUnfree = true;}
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
