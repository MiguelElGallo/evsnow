"""SQL template generation for Snowflake PAT and setup."""


def generate_pat_prerequisites_sql(user: str) -> str:
    """
    Generate SQL for PAT prerequisites (authentication policy).

    PAT requires either:
    1. A network policy attached to the user/account, OR
    2. An authentication policy with NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED

    This uses option 2 which is simpler for setup.

    Args:
        user: Snowflake username

    Returns:
        SQL statement to execute as ACCOUNTADMIN
    """
    return f"""\
-- ============================================================================
-- STEP 1: PAT PREREQUISITES
-- ============================================================================
-- PAT (Programmatic Access Token) requires network policy by default.
-- We create an authentication policy to bypass this requirement.
-- Run this as ACCOUNTADMIN in a Snowflake SQL worksheet.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create authentication policy that allows PAT without network policy
-- This also enables PAT as an authentication method
CREATE AUTHENTICATION POLICY IF NOT EXISTS evsnow_pat_policy
    AUTHENTICATION_METHODS = ('PROGRAMMATIC_ACCESS_TOKEN', 'PASSWORD', 'OAUTH')
    PAT_POLICY = (
        NETWORK_POLICY_EVALUATION = ENFORCED_NOT_REQUIRED
    )
    COMMENT = 'Authentication policy for EvSnow PAT setup';

-- Apply the policy to the user
ALTER USER {user} SET AUTHENTICATION POLICY evsnow_pat_policy;

-- Verify the policy is applied
DESCRIBE USER {user};
"""


def generate_pat_sql(
    user: str,
    token_name: str,
    days_to_expiry: int = 90,
) -> str:
    """
    Generate SQL to create a PAT for the specified user.

    Args:
        user: Snowflake username
        token_name: Name for the PAT
        days_to_expiry: Token validity period

    Returns:
        SQL statement to execute as ACCOUNTADMIN
    """
    return f"""\
-- ============================================================================
-- STEP 2: CREATE PROGRAMMATIC ACCESS TOKEN (PAT)
-- ============================================================================
-- Run this AFTER completing Step 1 (prerequisites).
-- Run this as ACCOUNTADMIN in a Snowflake SQL worksheet.
-- ============================================================================

USE ROLE ACCOUNTADMIN;

-- Create the PAT
ALTER USER {user} ADD PROGRAMMATIC ACCESS TOKEN {token_name}
    ROLE_RESTRICTION = 'ACCOUNTADMIN'
    DAYS_TO_EXPIRY = {days_to_expiry}
    COMMENT = 'PAT for EvSnow automated setup';

-- ============================================================================
-- IMPORTANT: The token_secret appears ONLY in this output!
-- Copy it immediately and store securely.
-- You cannot retrieve it again later.
-- ============================================================================
"""


def generate_verify_pat_sql(user: str) -> str:
    """Generate SQL to verify PAT tokens for a user."""
    return f"""\
-- List all PATs for user
SHOW USER PROGRAMMATIC ACCESS TOKENS FOR USER {user};
"""
