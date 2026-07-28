# Security

## Security Posture

Lua memcached assumes that callers, cache contents, cache servers, and network paths are trusted.
It is not designed to isolate mutually untrusted parties or safely process attacker-controlled data.
Applications are responsible for access control, transport protection, data compatibility, and
resource limits. Reports that require a violation of these assumptions are outside the project's
security scope, but may still be considered as ordinary robustness or correctness issues.
