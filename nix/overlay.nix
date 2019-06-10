{ profiling, haddocks }:

self: super:

with rec {
  inherit (super) lib;

  hlib = super.haskell.lib;

  # This function removes any cruft not relevant to our Haskell builds.
  #
  # If something irrelevant to our build is not removed by this function, and
  # you modify that file, Nix will rebuild the derivation even though nothing
  # that would affect the output has changed.
  #
  # The `excludePred` argument is a function that can be used to filter out more
  # files on a package-by-package basis.
  # The `includePred` argument is a function that can be used to include files
  # that this function would normally filter out.
  clean = (
    { path,
      excludePred ? (name: type: false),
      includePred ? (name: type: false)
    }:
    if lib.canCleanSource path
    then lib.cleanSourceWith {
           filter = name: type: (includePred name type) || !(
             with rec {
               baseName     = baseNameOf (toString name);
               isFile       = (type == "regular");
               isLink       = (type == "symlink");
               isDir        = (type == "directory");
               isUnknown    = (type == "unknown");
               isNamed      = str: (baseName == str);
               hasExtension = ext: (lib.hasSuffix ext baseName);
               beginsWith   = pre: (lib.hasPrefix pre baseName);
               matches      = regex: (builtins.match regex baseName != null);
             };

             lib.any (lib.all (x: x)) [
               # Each element of this list is a list of booleans, which should be
               # thought of as a "predicate" on paths; the predicate is true if the
               # list is composed entirely of true values.
               #
               # If any of these predicates is true, then the path will not be
               # included in the source used by the Nix build.
               #
               # Remember to use parentheses around elements of a list;
               # `[ f x ]`   is a heterogeneous list with two elements,
               # `[ (f x) ]` is a homogeneous list with one element.
               # Knowing the difference might save your life.
               [ (excludePred name type) ]
               [ isUnknown ]
               [ isDir (isNamed "dist") ]
               [ isDir (isNamed "dist-newstyle") ]
               [ isDir (isNamed  "run") ]
               [ (isFile || isLink) (hasExtension ".nix") ]
               [ (beginsWith ".ghc") ]
               [ (hasExtension ".sh") ]
               [ (hasExtension ".txt") ]
               [ (hasExtension ".project") ]
             ]);
           src = lib.cleanSource path;
         }
    else path);

  mainOverlay = hself: hsuper: {
    callC2N = (
      { name,
        path                  ? (throw "callC2N requires path argument!"),
        rawPath               ? (clean { inherit path; }),
        relativePath          ? null,
        args                  ? {},
        apply                 ? [],
        extraCabal2nixOptions ? []
      }:

      with rec {
        filter = p: type: (
          (super.lib.hasSuffix "${name}.cabal" p)
          || (baseNameOf p == "package.yaml"));
        expr = hsuper.haskellSrc2nix {
          inherit name;
          extraCabal2nixOptions = self.lib.concatStringsSep " " (
            (if relativePath == null then [] else ["--subpath" relativePath])
            ++ extraCabal2nixOptions);
          src = if super.lib.canCleanSource rawPath
                then super.lib.cleanSourceWith { src = rawPath; inherit filter; }
                else rawPath;
        };
        compose = f: g: x: f (g x);
        composeList = x: lib.foldl' compose lib.id x;
      };

      composeList apply
      (hlib.overrideCabal
       (hself.callPackage expr args)
       (orig: { src = rawPath; })));

    rotera = hlib.overrideCabal (hself.callC2N {
      name = "rotera";
      path = ../.;
      apply = [ hlib.dontCheck ]
        ++ ( if profiling
             then [ hlib.enableLibraryProfiling hlib.enableExecutableProfiling ]
             else [ hlib.disableLibraryProfiling hlib.disableExecutableProfiling ]
           )
        ++ ( if haddocks
             then [ hlib.doHaddock ]
             else [ hlib.dontHaddock ]
           );
    }) (old: {
      isLibrary = true;
      isExecutable = true;
      configureFlags = [
        "-f+application"
      ];
    });

    byte-order = hself.callC2N {
      name = "byte-order";
      rawPath = super.fetchFromGitHub {
        owner = "andrewthad";
        repo = "byte-order";
        rev = "91903467fbb5833990d6729c6650e5ff65765a16";
        sha256 = "0sjxldxh0i9yxgsgaia2aqm82yhk913b8379zdi2wcw7yw791v1p";
      };
      apply = [ ];
    };

    byteslice = hself.callC2N {
      name = "byteslice";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "byteslice";
        rev = "08740bf22e4ebf1f650cd825dec5b9dd9833b9c2";
        sha256 = "17pynssq5lxka27i2wikhw87dlxj6kaf6kmbhf00zdpgq4xd8095";
      };
      apply = [ ];
    };

    mmap = hself.callC2N {
      name = "mmap";
      rawPath = super.fetchFromGitHub {
        owner = "andrewthad";
        repo = "mmap";
        rev = "5454d948f51701f70327f15b2206e8d7fda51da1";
        sha256 = "1gkvin9gqz3447rs5gw9pi604g53qrhqwfgbr0xgsb9ygxgj5apj";
      };
      apply = [ ];
    };

    primitive = hself.callC2N {
      name = "primitive";
      rawPath = super.fetchFromGitHub {
        owner  = "haskell";
        repo   = "primitive";
        rev    = "47c4341f4e0f6fbbc3c53f23ca71e588c9515e6d";
        sha256 = "1mjdnykylbjgb15vbl6ha5jdqz2c9jyd9yxlqavvinmdglxgrc5j";
      };
      apply = [ hlib.dontCheck ];
    };

    primitive-checked = hself.callC2N {
      name = "primitive-checked";
      rawPath = super.fetchFromGitHub {
        owner  = "haskell-primitive";
        repo   = "primitive-checked";
        rev = "d136c5223be331c21c51769eb0be0c85bc6feee6";
        sha256 = "179knm68zl0ajj744j7g8n8fxd6gjwmv5128xmh9ci7hg6diwarm";
      };
      apply = [ hlib.dontCheck ];
    };

    primitive-addr = hself.callC2N {
      name = "primitive-addr";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "primitive-addr";
        rev    = "242dc71947838afe89a02b4f57d96f96c06145a1";
        sha256 = "04k563b58rvgidhdvd6ri65gsp5bdcxm8zpr9lksf9nq1rvsf9av";
      };
      apply = [ ];
    };

    primitive-offset = hself.callC2N {
      name = "primitive-offset";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "primitive-offset";
        rev = "dfc7b35285731b8589023abf398d45cdcf9138ab";
        sha256 = "0jfn42xav31zs9yd5841qy76qrc25cbx6lpgfh5av3v6dsxyrxb7";
      };
      apply = [ ];
    };

    primitive-unlifted = hself.callC2N {
      name = "primitive-unlifted";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "primitive-unlifted";
        rev = "fb93ffd7e1703ca9ff5b616a86e1ca1e42683064";
        sha256 = "1k3xh89xl0vymnlad4z0lpyq389znk19mwc3zrbr3j4cg0284i4z";
      };
      apply = [ hlib.dontCheck ];
    };

    sockets = hlib.overrideCabal (hself.callC2N {
      name = "sockets";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "sockets";
        rev = "a8056a390a5657a563e272e2dead859fc9a4ee15";
        sha256 = "0bk5vbcglcjkpw30rsd3iaj7zp2lrqp13q4v1m8d55wmsnmp202d";
      };
      apply = [ hlib.dontHaddock ];
    }) (old : {
      configureFlags = [
        # GHC cannot perform multithreaded backpackified typechecking
        # Nix's generic haskell builder invokes ghc with `j$NIX_BUILD_CORES`
        # by default. Anything other than 1 behind that value results in
        # GHC giving up when attempting to compile `sockets`.
        "--ghc-option=-j1"
        "-f+verbose-errors"
      ];
    });

    posix-api = hself.callC2N {
      name = "posix-api";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "posix-api";
        rev = "64c591d71fca86a58c04e15b187d5580021bebc4";
        sha256 = "0ia6xb4jcgb8v76mvmih6w2rsc4a0jshxn4y98xkn8wh2bsypgyj";
      };
      apply = [ hlib.dontCheck ];
    };

    primitive-sort = hself.callC2N {
      name = "primitive-sort";
      rawPath = super.fetchFromGitHub {
        owner = "chessai";
        repo = "primitive-sort";
        rev = "79a411dbcfa288d2d6635271a1286ccbbe52da3c";
        sha256 = "1xclbnywr110xzg92f3ni1ip39mn6nqmcd5hhn9dvnm91wka3h5z";
      };
      apply = [ hlib.dontCheck ];
    };

    time-compat = hself.callC2N {
      name = "time-compat";
      rawPath = super.fetchFromGitHub {
        owner = "phadej";
        repo = "time-compat";
        rev = "89ef24ecf2b9a7f30bf91ec7cc82edc71c7b29d0";
        sha256 = "0ny2i1yv2pqgl6ksj8wg561hi6xdrlg5lp1z7qwrqdbjk9ymkcc0";
      };
      apply = [ hlib.dontCheck ];
    };

    aeson = hself.callC2N {
      name = "aeson";
      rawPath = super.fetchFromGitHub {
        owner  = "bos";
        repo   = "aeson";
        sha256 = "0y64d6mn82v2z827v4kpi5b6ylhxczkqgiww6bjdqb9acpwlbqih";
        rev = "c1907c9013a5a54dbc50838cb93be97ded5c8c01";
      };
      apply = [ hlib.doJailbreak hlib.dontHaddock hlib.dontCheck hlib.dontBenchmark ];
    };

    country = hlib.doJailbreak hsuper.country;

    semirings = hsuper.semirings_0_3_1_2;

    http-client = hself.callC2N {
      name = "http-client";
      rawPath = super.fetchFromGitHub {
        owner  = "snoyberg";
        repo   = "http-client";
        rev    = "ad4fa206c5bd40cd9e1ab6eb22f005f5e011e7d4";
        sha256 = "1bf0bi97hbyphfdaq21w24vl3pqwlmij9gf79fid6grpyk6gkvmf";
      };
      apply = [ ];
      relativePath = "http-client";
    };

    http-client-tls = hself.callC2N {
      name = "http-client-tls";
      rawPath = super.fetchFromGitHub {
        owner  = "snoyberg";
        repo   = "http-client";
        rev    = "ad4fa206c5bd40cd9e1ab6eb22f005f5e011e7d4";
        sha256 = "1bf0bi97hbyphfdaq21w24vl3pqwlmij9gf79fid6grpyk6gkvmf";
      };
      apply = [ ];
      relativePath = "http-client-tls";
    };

    http-conduit = hself.callC2N {
      name = "http-conduit";
      rawPath = super.fetchFromGitHub {
        owner  = "snoyberg";
        repo   = "http-conduit";
        rev    = "ad4fa206c5bd40cd9e1ab6eb22f005f5e011e7d4";
        sha256 = "1bf0bi97hbyphfdaq21w24vl3pqwlmij9gf79fid6grpyk6gkvmf";
      };
      apply = [ ];
      relativePath = "http-conduit";
    };

    uuid = hself.callC2N {
      name = "uuid";
      rawPath = super.fetchFromGitHub {
        owner  = "haskell-hvr";
        repo   = "uuid";
        rev    = "fc16f9c84169e91b323185147ec955c3504f65c3";
        sha256 = "1d2ckc8pmg29hq13r9f4mv7bh0inqsar62k93sfyy9gb6c1j470l";
      };
      apply = [ hlib.doJailbreak ];
      relativePath = "uuid";
    };

    uuid-types = hself.callC2N {
      name = "uuid-types";
      rawPath = super.fetchFromGitHub {
        owner  = "haskell-hvr";
        repo   = "uuid";
        rev    = "fc16f9c84169e91b323185147ec955c3504f65c3";
        sha256 = "1d2ckc8pmg29hq13r9f4mv7bh0inqsar62k93sfyy9gb6c1j470l";
      };
      apply = [ hlib.doJailbreak ];
      relativePath = "uuid-types";
    };

    fast-logger = hself.callC2N {
      name = "fast-logger";
      rawPath = super.fetchFromGitHub {
        owner  = "kazu-yamamoto";
        repo   = "logger";
        rev    = "1b22109c36c6c78874e088d881c5173ef9036622";
        sha256 = "03l9b5jaqrg0y6d6bl53hp1wc5y9dwydk3xl6s6r4wyfimvpschk";
      };
      apply = [ hlib.dontHaddock ];
      relativePath = "fast-logger";
    };

    primitive-atomic = hself.callC2N {
      name = "primitive-atomic";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "primitive-atomic";
        rev = "4b9ec2f26ff3252f000482342d3fec3402f48d0b";
        sha256 = "1hc6rqjjc26f6pm8y2c7cay6fgaffmbal9vx2g110vw10pndrw80";
      };
      apply = [ hlib.dontHaddock ];
    };

    contiguous = hself.callC2N {
      name = "contiguous";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "contiguous";
        rev = "4bade9b139ebef8b2419ce0426dce27613084071";
        sha256 = "13r1qnkg39zk1zdwslh3zkmzg9n77yxbipjavwadqm15qm6zscjr";
      };
      apply = [ ];
    };

    error-codes = hself.callC2N {
      name = "error-codes";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "error-codes";
        rev = "5eb520f475285eeed17fe33f4bac5929104657a0";
        sha256 = "0shcvsyykbpwjsd9nwnyxkp298wmfpa7v2h8vw1clhka2xsw2c86";
      };
      apply = [ ];
    };

    primitive-containers = hself.callC2N {
      name = "primitive-containers";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "primitive-containers";
        rev = "c7e9a4deac7d534ca2b0d8fd910e29c2ba3048ab";
        sha256 = "1kz1cj2hb3g7gq8blnqq2j8mhb8ma986i2g6p1lqmdjshjhi850r";
      };
      apply = [ ];
    };

    ip = hself.callC2N {
      name = "ip";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "haskell-ip";
        rev = "2fe1a38d1bf2155cf068ac3b7e08fa7319d4231c";
        sha256 = "13z01jryfkfj9z7d45nsb55v6798gv9dqqrqw5wxymzywmhqyc4m";
      };
      apply = [ ];
    };

    chronos = hself.callC2N {
      name = "chronos";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "chronos";
        rev = "5e3d263f0c5a6a41ae5b58a95712f21abdf4bed5";
        sha256 = "0ai8hi839n6sj4bqrqzyydkgy4vsck9kg4037ajriz9z4a40zj6g";
      };
      apply = [ ];
    };

    wide-word = hself.callC2N {
      name = "wide-word";
      rawPath = super.fetchFromGitHub {
        owner = "erikd";
        repo = "wide-word";
        rev = "f216c223c6ae4fb3854803c39f1244686cb06353";
        sha256 = "1bn6ikqvhbsh6j6qrvcdl9avcgp2128an0mjv7ckspxanx2avpip";
      };
      apply = [ ];
    };

    network = hlib.dontCheck hsuper.network;

    # requires mtl < 1.2 and network < 2.8
    HTTP = hself.callC2N {
      name = "HTTP";
      rawPath = super.fetchFromGitHub {
        owner  = "chessai";
        repo   = "HTTP";
        rev    = "9ed6f1fae736cc18187fc7e3c2865f025952385a";
        sha256 = "0rpxj57wnh85f4y8gq6ix7wf91m2p45q9ngwjw9zrn56jy64ls83";
      };
      apply = [ hlib.dontCheck ];
    };

    quickcheck-classes = hself.callC2N {
      name = "quickcheck-classes";
      rawPath = super.fetchFromGitHub {
        owner  = "andrewthad";
        repo   = "quickcheck-classes";
        rev = "6d79445a8824accd827239587ca1c98d40b02692";
        sha256 = "10k9x5v5b19dzqlzmzi7mk3qfigjkb00rhrm2sh5s2c73rifpy84";
      };
      apply = [ hlib.doJailbreak hlib.dontCheck ];

    };

    comonad           = hlib.disableCabalFlag hsuper.comonad "test-doctests";
    semigroupoids     = hlib.disableCabalFlag hsuper.semigroupoids "doctests";
    lens              = hlib.disableCabalFlag hsuper.lens "test-doctests";
    distributive      = hlib.dontCheck (hlib.disableCabalFlag hsuper.distributive "test-doctests");

    http-types        = hlib.dontCheck hsuper.http-types;
    silently          = hlib.dontCheck hsuper.silently;
    unliftio          = hlib.dontCheck hsuper.unliftio;
    conduit           = hlib.dontCheck hsuper.conduit;
    yaml              = hlib.dontCheck hsuper.yaml;
    extra             = hlib.dontCheck hsuper.extra;
    half              = hlib.dontCheck hsuper.half;
    iproute           = hlib.dontCheck hsuper.iproute;
    aeson-compat      = hlib.dontCheck hsuper.aeson-compat;
    tzdata            = hlib.dontCheck hsuper.tzdata;
    tz                = hlib.dontCheck hsuper.tz;
    time-exts         = hlib.dontCheck hsuper.time-exts;
    double-conversion = hlib.dontCheck hsuper.double-conversion;
    cron              = hlib.dontCheck hsuper.cron;
    vector-algorithms = hlib.doJailbreak hsuper.vector-algorithms;
    foldl = hlib.doJailbreak hsuper.foldl;
  };

  composeOverlayList = lib.foldl' lib.composeExtensions (_: _: {});

  overlay = composeOverlayList [
    mainOverlay
  ];

};

{
  haskell = super.haskell // {
    packages = super.haskell.packages // {
      ghc844 = (super.haskell.packages.ghc844.override {
        overrides = super.lib.composeExtensions
          (super.haskell.packageOverrides or (self: super: {}))
          overlay;
      });
      ghc864 = (super.haskell.packages.ghc864.override {
        overrides = super.lib.composeExtensions
          (super.haskell.packageOverrides or (self: super: {}))
          overlay;
      });
      ghc865 = (super.haskell.packages.ghc865.override {
        overrides = super.lib.composeExtensions
          (super.haskell.packageOverrides or (self: super: {}))
          overlay;
      });
      ghcjs86 = (super.haskell.packages.ghcjs86.override {
        overrides = super.lib.composeExtensions
          (super.haskell.packageOverrides or (self: super: {}))
          overlay;
      });
    };
  };

}
