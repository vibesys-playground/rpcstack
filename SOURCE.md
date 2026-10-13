# Source attribution

Extracted from https://github.com/vic-lsh/masa at
`b31330e1529af34d2609631c15c2c6a928d5a842` (framework PR #103).
This repository intentionally starts with one initial commit and imports no Git
history. The initial extraction excluded source policy implementations and applications.
Later optional example ports are separately attributed in their READMEs; the
framework still has no policy dependency.

The extraction preserves the four rpcstack components and their necessary runtime
support. Generic runtime/hook namespaces and transport feature names were renamed
to `rpcstack`; generated stubs default to no-op hooks; the legacy policy-specific
header decoder and separate application-facing channel were removed. Missing or
invalid scheduling keys now use the scheduler's default spawn metadata.

Vendored Tokio, Tonic, and Hyper retain their upstream copyright and license files.
This extraction does not grant a new license to files that had no license grant in
the source snapshot. Refer to the original source and component notices for terms.
