-- Widen the runtime_profile.protocol_family CHECK constraint to include DimCode (dimcli).
-- dimcli follows the same ACP-native pattern as Traecli (migration 136) and Qoder (migration 134):
-- it has a New() backend, a launch header ("dim acp"), and provider branding, but was missing from
-- the protocol_family whitelist, so custom runtime profiles based on DimCode were rejected and it
-- never appeared in the family picker.
ALTER TABLE runtime_profile DROP CONSTRAINT IF EXISTS runtime_profile_protocol_family_check;

ALTER TABLE runtime_profile ADD CONSTRAINT runtime_profile_protocol_family_check
    CHECK (protocol_family IN (
        'claude',
        'codebuddy',
        'codex',
        'copilot',
        'opencode',
        'openclaw',
        'hermes',
        'pi',
        'cursor',
        'kimi',
        'kiro',
        'antigravity',
        'qoder',
        'traecli',
        'dimcli'
    )) NOT VALID;
