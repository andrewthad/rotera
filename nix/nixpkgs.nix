let
  fetchNixpkgs = import ./fetchNixpkgs.nix;

  nixpkgs = fetchNixpkgs {
    owner = "NixOS";
    repo = "nixpkgs";
    rev = "9a8d764486a5fc800d9ef202543f7bb7bb5f8ae7";
    sha256 = "0b5qp0kmqgwgjal5ykvazbq1fglpk9faj79igyjmldbqs3gq62w3";
  };

in
  nixpkgs
