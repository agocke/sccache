#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="x86_64-unknown-linux-musl"
CONFIGURATION="release"

# Read version from Cargo.toml
VERSION=$(grep -m1 '^version' "$SCRIPT_DIR/Cargo.toml" | sed 's/version = "\(.*\)"/\1/')
PACKAGE_ID="sccache"

echo "==> Building sccache v${VERSION} for ${TARGET}..."
cargo build --release --target "$TARGET" --features vendored-openssl

BINARY="$SCRIPT_DIR/target/${TARGET}/${CONFIGURATION}/sccache"
if [[ ! -f "$BINARY" ]]; then
    echo "ERROR: Binary not found at $BINARY"
    exit 1
fi

# Set up staging directory
STAGING_DIR="$SCRIPT_DIR/target/nuget-staging"
rm -rf "$STAGING_DIR"
mkdir -p "$STAGING_DIR/tools"

cp "$BINARY" "$STAGING_DIR/tools/sccache"
chmod +x "$STAGING_DIR/tools/sccache"

# Generate package README
cat > "$STAGING_DIR/README.md" <<'READMEEOF'
# sccache

This package contains a linux-musl-x64 build of [sccache](https://github.com/mozilla/sccache),
built from the [agocke/sccache](https://github.com/agocke/sccache) fork.

This is **not** an official Mozilla build. It is maintained independently and may
contain changes not present in the upstream repository.

## Usage

The `sccache` binary is located in the `tools/` directory of this package.
READMEEOF

# Generate a minimal .csproj for dotnet pack
cat > "$STAGING_DIR/${PACKAGE_ID}.csproj" <<EOF
<Project Sdk="Microsoft.NET.Sdk">
  <PropertyGroup>
    <TargetFramework>net8.0</TargetFramework>
    <PackageId>${PACKAGE_ID}</PackageId>
    <PackageVersion>${VERSION}</PackageVersion>
    <Authors>Andy Gocke;Mozilla</Authors>
    <Description>sccache - shared compilation cache (linux-musl-x64)</Description>
    <PackageLicenseExpression>Apache-2.0</PackageLicenseExpression>
    <PackageProjectUrl>https://github.com/agocke/sccache</PackageProjectUrl>
    <RepositoryUrl>https://github.com/agocke/sccache</RepositoryUrl>
    <RepositoryType>git</RepositoryType>
    <!-- Suppress warnings and don't build any code -->
    <NoBuild>true</NoBuild>
    <IncludeBuildOutput>false</IncludeBuildOutput>
    <SuppressDependenciesWhenPacking>true</SuppressDependenciesWhenPacking>
    <GenerateAssemblyInfo>false</GenerateAssemblyInfo>
    <PackageReadmeFile>README.md</PackageReadmeFile>
  </PropertyGroup>
  <ItemGroup>
    <Content Include="tools/**" PackagePath="tools" />
    <None Include="README.md" Pack="true" PackagePath="" />
  </ItemGroup>
</Project>
EOF

# Pack
OUTPUT_DIR="$SCRIPT_DIR/target/nuget"
mkdir -p "$OUTPUT_DIR"

echo "==> Packing NuGet package..."
dotnet restore "$STAGING_DIR/${PACKAGE_ID}.csproj" --verbosity quiet
dotnet pack "$STAGING_DIR/${PACKAGE_ID}.csproj" \
    --no-build \
    --output "$OUTPUT_DIR"

NUPKG="$OUTPUT_DIR/${PACKAGE_ID}.${VERSION}.nupkg"
echo "==> Package created: $NUPKG"
ls -lh "$NUPKG"

# Clean up staging
rm -rf "$STAGING_DIR"
echo "==> Done."
