# GitHub Copilot SDK Authentication

## Overview

The GitHub Copilot SDK requires authentication to use the Copilot CLI server that powers the SDK. This document covers the **simplest authentication option** for getting started with the GitHub Copilot SDK.

## Simplest Authentication Method: Environment Variable with Personal Access Token

The easiest and most straightforward way to authenticate the GitHub Copilot SDK is to use the `COPILOT_GITHUB_TOKEN` environment variable with a GitHub Personal Access Token (PAT).

### Step-by-Step Setup

#### 1. Generate a Personal Access Token

1. Go to [GitHub's personal access token page](https://github.com/settings/tokens) and choose "Fine-grained tokens" (recommended) or visit [direct fine-grained token creation](https://github.com/settings/personal-access-tokens/new)
2. Click "Generate new token"
3. Give your token a descriptive name (e.g., "Copilot SDK Access")
4. Set an expiration date based on your needs
5. Under "Permissions" or "Repository permissions", look for and enable GitHub Copilot access (the exact permission name may vary - look for options containing "Copilot")
6. Click "Generate token"
7. **Important**: Copy the token immediately - you won't be able to see it again!

#### 2. Set the Environment Variable

Export the token as an environment variable before running your application:

**Linux/macOS:**
```bash
export COPILOT_GITHUB_TOKEN="ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

**Windows Command Prompt:**
```cmd
set COPILOT_GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx
```

**Windows PowerShell:**
```powershell
$env:COPILOT_GITHUB_TOKEN="ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
```

#### 3. Run Your Application

Once the environment variable is set, the SDK will automatically detect and use it for authentication. No additional configuration is needed in your code.

### Why This Method is Simplest

- **No interactive login**: Unlike OAuth device flow, you don't need to authenticate through a browser
- **No CLI setup required**: You don't need to authenticate with the GitHub CLI (`gh`) separately
- **Highest priority**: `COPILOT_GITHUB_TOKEN` takes precedence over other authentication methods, avoiding conflicts
- **Works everywhere**: Compatible with local development, CI/CD pipelines, and containerized environments

### Prerequisites

- A valid GitHub Copilot subscription (required for all SDK usage)
- The GitHub Copilot CLI installed on your system (see [installation guide](https://docs.github.com/en/copilot/how-tos/set-up/install-copilot-cli))

### Authentication Priority

If you're curious about other authentication methods, the SDK checks for authentication in this order:

1. `COPILOT_GITHUB_TOKEN` environment variable (recommended - **this method**)
2. `GH_TOKEN` environment variable
3. `GITHUB_TOKEN` environment variable
4. GitHub CLI (`gh`) authenticated session
5. OAuth device authorization flow (interactive browser login)

Using `COPILOT_GITHUB_TOKEN` ensures your authentication is explicit and won't be overridden by other tools.

### Troubleshooting

**Token not recognized:**
- Verify your token includes GitHub Copilot access permissions (the exact name varies, but look for Copilot-related scopes)
- Ensure your Copilot subscription is active and associated with your GitHub account
- Check that the environment variable is properly set:
  - Linux/macOS: `echo $COPILOT_GITHUB_TOKEN`
  - Windows Command Prompt: `echo %COPILOT_GITHUB_TOKEN%`
  - Windows PowerShell: `echo $env:COPILOT_GITHUB_TOKEN`

**Conflicts with other tokens:**
- If you have `GH_TOKEN` or `GITHUB_TOKEN` set, consider unsetting them to avoid confusion
- `COPILOT_GITHUB_TOKEN` will take priority, but it's cleaner to have only one authentication method active

### For CI/CD Environments

Store your token as a secret in your CI/CD platform and inject it as an environment variable:

**GitHub Actions example:**
```yaml
env:
  COPILOT_GITHUB_TOKEN: ${{ secrets.COPILOT_GITHUB_TOKEN }}
```

## Summary

The simplest way to authenticate with the GitHub Copilot SDK is:
1. Create a Personal Access Token with "Copilot Requests" permission
2. Export it as `COPILOT_GITHUB_TOKEN` environment variable
3. The SDK handles the rest automatically

That's it! No complex OAuth flows, no CLI configuration - just set the environment variable and start using the SDK.
