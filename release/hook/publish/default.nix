{ pkgs, config }:
let
 name = "hcp-release-hook-publish";

 script = pkgs.writeShellScriptBin name ''
set -euox pipefail
echo "packaging for crates.io"
# order is important here due to dependencies
for crate in \
 holochain_persistence_api \
 holochain_persistence_file \
 holochain_persistence_mem \
 holochain_persistence_pickle
do
 cargo package --manifest-path "crates/$crate/Cargo.toml"
 cargo publish --manifest-path "crates/$crate/Cargo.toml"

 sleep 10
 while ! cargo search --registry crates-io -- $crate | grep -q "$crate = \"${config.release.version.current}\"";
 do
  echo 'waiting for crates.io to finish publishing ${config.release.version.current}'
  sleep 10
 done
done
'';
in
{
 buildInputs = [ script ];
}
