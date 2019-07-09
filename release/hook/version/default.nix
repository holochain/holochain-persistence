{ pkgs, config }:
let
 name = "hcp-release-hook-version";

 script = pkgs.writeShellScriptBin name ''
echo "bumping holochain_persistence_api dependency versions to ${config.release.version.current} in all Cargo.toml"
find . \
 -name "Cargo.toml" \
 -not -path "**/target/**" \
 -not -path "**/.git/**" \
 -not -path "**/.cargo/**" | xargs -I {} \
 sed -i 's/^holochain_persistence_api = { version = "=[0-9]\+.[0-9]\+.[0-9]\+\(-alpha[0-9]\+\)\?"/holochain_persistence_api = { version = "=${config.release.version.current}"/g' {}
'';
in
{
 buildInputs = [ script ];
}
