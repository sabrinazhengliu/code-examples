# GitHub Actions Testing with Native Git Integration

This guide sets up automated notebook testing using:
- Snowflake service account with key pair authentication
- Native GitHub integration (notebooks stay in GitHub)
- GitHub Actions for CI/CD

---
## PART 1: SNOWFLAKE SETUP

Step 1: Create GitHub Personal Access Token
--------------------------------------------
1. Go to: https://github.com/settings/tokens
2. Click "Generate new token" → "Generate new token (classic)"
3. Select scopes:
   ✓ repo (full control)
4. Generate and copy token (save it - you'll need it)

Step 2: Create API Integration (One-time, ACCOUNTADMIN)
--------------------------------------------------------
```sql
-- Switch to ACCOUNTADMIN role
USE ROLE ACCOUNTADMIN;

-- Create API integration for GitHub
CREATE OR REPLACE API INTEGRATION github_integration
  API_PROVIDER = git_https_api
  API_ALLOWED_PREFIXES = ('https://github.com/your-org/')
  ALLOWED_AUTHENTICATION_SECRETS = ALL
  ENABLED = TRUE
  COMMENT = 'API integration for GitHub repositories';

-- Verify creation
DESC API INTEGRATION github_integration;
SHOW API INTEGRATIONS LIKE 'github_integration';
```

Step 3: Create Secret for GitHub Authentication
------------------------------------------------
```sql
-- Create secret with GitHub credentials
CREATE OR REPLACE SECRET github_secret
  TYPE = PASSWORD
  USERNAME = 'your-github-username'
  PASSWORD = 'ghp_your_github_personal_access_token';

-- Verify (won't show password)
DESC SECRET github_secret;
```

Step 4: Create Git Repository Object
-------------------------------------
```sql
-- Create database and schema if needed
CREATE DATABASE IF NOT EXISTS mydb;
CREATE SCHEMA IF NOT EXISTS mydb.public;

USE DATABASE mydb;
USE SCHEMA public;

-- Create Git repository pointing to your GitHub repo
CREATE OR REPLACE GIT REPOSITORY mydb.public.notebooks_repo
  API_INTEGRATION = github_integration
  ORIGIN = 'https://github.com/your-org/your-project'
  GIT_CREDENTIALS = github_secret
  COMMENT = 'GitHub repository for notebooks';

-- Fetch repository content
ALTER GIT REPOSITORY mydb.public.notebooks_repo FETCH;

-- Verify files are accessible
SHOW FILES IN @mydb.public.notebooks_repo/branches/main/;
LS @mydb.public.notebooks_repo/branches/main/notebooks/;
```

Step 5: Create and Configure Role
----------------------------------
```
-- Create role for GitHub Actions
CREATE ROLE IF NOT EXISTS github_actions_role
  COMMENT = 'Role for GitHub Actions automation';

-- Grant warehouse access
GRANT USAGE ON WAREHOUSE compute_wh TO ROLE github_actions_role;

-- Grant database and schema access
GRANT USAGE ON DATABASE mydb TO ROLE github_actions_role;
GRANT USAGE ON SCHEMA mydb.public TO ROLE github_actions_role;

-- Grant Git repository access
GRANT READ ON GIT REPOSITORY mydb.public.notebooks_repo TO ROLE github_actions_role;

-- Grant notebook privileges
GRANT CREATE NOTEBOOK ON SCHEMA mydb.public TO ROLE github_actions_role;
GRANT EXECUTE NOTEBOOK ON SCHEMA mydb.public TO ROLE github_actions_role;

-- Grant table privileges (if notebooks read/write tables)
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA mydb.public 
  TO ROLE github_actions_role;
GRANT SELECT, INSERT, UPDATE, DELETE ON FUTURE TABLES IN SCHEMA mydb.public 
  TO ROLE github_actions_role;

-- Grant API integration usage (needed for Git operations)
GRANT USAGE ON INTEGRATION github_integration TO ROLE github_actions_role;
```

Step 6: Generate Key Pair for Service Account
----------------------------------------------
-- Run on your local machine (not in Snowflake)

```
# Generate private key (2048-bit RSA)
openssl genrsa -out snowflake_github_actions.pem 2048

# Generate public key
openssl rsa -in snowflake_github_actions.pem -pubout -out snowflake_github_actions.pub

# Extract public key for Snowflake (remove headers/newlines)
grep -v "BEGIN\|END" snowflake_github_actions.pub | tr -d '\n'

# Save output - you'll use this in next step

# Extract private key for GitHub secret (remove headers/newlines)
grep -v "BEGIN\|END\|PRIVATE" snowflake_github_actions.pem | tr -d '\n'

# Save output - you'll add this to GitHub secrets
```

Step 7: Create Service Account User
------------------------------------
```sql
USE ROLE ACCOUNTADMIN;

-- Create service account for GitHub Actions
CREATE USER IF NOT EXISTS github_actions_user
  TYPE = SERVICE
  COMMENT = 'Service account for GitHub Actions CI/CD';

-- Assign role to user
GRANT ROLE github_actions_role TO USER github_actions_user;

-- Verify grants
SHOW GRANTS TO ROLE github_actions_role;
SHOW GRANTS TO USER github_actions_user;

-- Set default role for service account user
ALTER USER github_actions_user
  DEFAULT_ROLE = github_actions_role;

-- Set public key (replace with your actual public key from Step 5)
ALTER USER github_actions_user 
  SET RSA_PUBLIC_KEY = 'MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA...';

-- Verify user creation
SHOW USERS LIKE 'github_actions_user';
DESC USER github_actions_user;
```

Step 8: Test Service Account Authentication
--------------------------------------------
```
-- Test on your local machine first

export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=github_actions_user
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
export SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowflake_github_actions.pem

# Test connection
snow connection test -x

# Test Git repository access
snow sql -q "SHOW FILES IN @mydb.public.notebooks_repo/branches/main/notebooks/" -x

# Test notebook execution from Git
snow git execute "@mydb.public.notebooks_repo/branches/main/notebooks/analysis.ipynb" -x
```

---
## PART 2: GITHUB REPOSITORY SETUP

Step 9: Repository Structure
-----------------------------
Ensure your repository has this structure:

```
your-repo/ 
├── ANALYSIS/← Folder per notebook
│   ├── ANALYSIS.ipynb
│   └── metadata.json
│   └── utility.py
├── ETL_PIPELINE/← Another folder
│   ├── ETL_PIPELINE.ipynb
│   └── config.py
└── DATA_PROCESSING/
│   ├── DATA_PROCESSING.ipynb
│   └── metadata.json
└── README.md
```

Step 10: Configure GitHub Secrets
----------------------------------
1. Go to: https://github.com/your-org/your-project/settings/secrets/actions
2. Click "New repository secret"
3. Add the following secrets:

* Secret Name: SNOWFLAKE_ACCOUNT
   * Value: abc12345.us-east-1

* Secret Name: SNOWFLAKE_USER
   * Value: github_actions_user

* Secret Name: SNOWFLAKE_PRIVATE_KEY_RAW
   * Value: (paste the private key string from Step 5 - without headers/newlines)

* Secret Name: SNOWFLAKE_DATABASE
   * Value: MYDB

* Secret Name: SNOWFLAKE_SCHEMA
   * Value: PUBLIC

* Secret Name: SNOWFLAKE_WAREHOUSE
   * Value: COMPUTE_WH

* Secret Name: SNOWFLAKE_GIT_REPO
   * Value: mydb.public.notebooks_repo

Step 11: Create GitHub Actions Workflow
----------------------------------------
Create file: .github/workflows/test-notebooks.yml

```yaml
name: Test Snowflake Notebooks

on:
  push:
    branches: [main, develop]
    paths:
      - 'notebooks/**'
  pull_request:
    branches: [main]
  workflow_dispatch:  # Manual trigger

jobs:
  test-notebooks:
    runs-on: ubuntu-latest
    
    steps:
      - name: Setup Snowflake CLI
        uses: snowflakedb/snowflake-cli-action@v2.0
        with:
          cli-version: "3.11.0"

      - name: Test Snowflake connection
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
        run: |
          echo "Testing connection to Snowflake..."
          snow connection test -x

      - name: Sync Git repository
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
          SNOWFLAKE_GIT_REPO: ${{ secrets.SNOWFLAKE_GIT_REPO }}
        run: |
          echo "Syncing Git repository with latest changes..."
          snow sql -q "ALTER GIT REPOSITORY $SNOWFLAKE_GIT_REPO FETCH" -x
          
          echo "Listing notebooks in repository..."
          snow sql -q "LS @$SNOWFLAKE_GIT_REPO/branches/main/notebooks/" -x

      - name: Execute notebooks from Git repository
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
          SNOWFLAKE_GIT_REPO: ${{ secrets.SNOWFLAKE_GIT_REPO }}
          SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
          SNOWFLAKE_SCHEMA: ${{ secrets.SNOWFLAKE_SCHEMA }}
          SNOWFLAKE_WAREHOUSE: ${{ secrets.SNOWFLAKE_WAREHOUSE }}
        run: |
          # Define notebooks to test
          NOTEBOOKS=(
            "analysis"
            "etl_pipeline"
            "data_processing"
          )
          
          failed_notebooks=()
          
          for notebook in "${NOTEBOOKS[@]}"; do
            echo "=========================================="
            echo "Executing: $notebook"
            echo "=========================================="

            if snow sql -q "EXECUTE NOTEBOOK ${notebook}"
              --database "$SNOWFLAKE_DATABASE" \
              --schema "$SNOWFLAKE_SCHEMA" \
              --warehouse "$SNOWFLAKE_WAREHOUSE" \
              -x; then
              echo "✅ SUCCESS: $notebook"
            else
              echo "❌ FAILED: $notebook"
              failed_notebooks+=("$notebook")
            fi
          done
          
          # Report results
          echo "=========================================="
          echo "Test Summary"
          echo "=========================================="
          if [ ${#failed_notebooks[@]} -eq 0 ]; then
            echo "✅ All notebooks passed"
          else
            echo "❌ Failed notebooks: ${failed_notebooks[*]}"
            exit 1
          fi

      - name: Verify notebook execution
        if: success()
        env:
          SNOWFLAKE_ACCOUNT: ${{ secrets.SNOWFLAKE_ACCOUNT }}
          SNOWFLAKE_USER: ${{ secrets.SNOWFLAKE_USER }}
          SNOWFLAKE_AUTHENTICATOR: SNOWFLAKE_JWT
          SNOWFLAKE_PRIVATE_KEY_RAW: ${{ secrets.SNOWFLAKE_PRIVATE_KEY_RAW }}
          SNOWFLAKE_DATABASE: ${{ secrets.SNOWFLAKE_DATABASE }}
          SNOWFLAKE_SCHEMA: ${{ secrets.SNOWFLAKE_SCHEMA }}
        run: |
          echo "Verifying notebooks in Snowflake..."
          snow sql -q "SHOW NOTEBOOKS IN SCHEMA $SNOWFLAKE_DATABASE.$SNOWFLAKE_SCHEMA" -x
```

## PART 3: TESTING AND VALIDATION

Step 12: Local Testing (Before Pushing)
----------------------------------------
```
# Set environment variables
export SNOWFLAKE_ACCOUNT=abc12345.us-east-1
export SNOWFLAKE_USER=github_actions_user
export SNOWFLAKE_AUTHENTICATOR=SNOWFLAKE_JWT
export SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/snowflake_github_actions.pem
export SNOWFLAKE_GIT_REPO=mydb.public.notebooks_repo

# Test connection
snow connection test -x

# Test Git sync
snow sql -q "ALTER GIT REPOSITORY $SNOWFLAKE_GIT_REPO FETCH" -x

# Test notebook execution
snow git execute "@$SNOWFLAKE_GIT_REPO/branches/main/notebooks/analysis.ipynb" -x
```

Step 13: Push and Monitor
--------------------------
```
# Add workflow file
git add .github/workflows/test-notebooks.yml

# Commit
git commit -m "Add GitHub Actions workflow for notebook testing"

# Push to trigger workflow
git push origin main
```

#### Monitor workflow
Go to: https://github.com/your-org/your-project/actions

Step 14: Troubleshooting
-------------------------
* **Issue: Authentication failed**
* Solution: 
  - Verify public key is set on user
  - Check private key in GitHub secrets has no headers/newlines
  - Run: DESC USER github_actions_user; (verify RSA_PUBLIC_KEY is set)

* **Issue: Permission denied on Git repository**
* Solution:
  - Check grants: SHOW GRANTS TO ROLE github_actions_role;
  - Grant READ access: GRANT READ ON GIT REPOSITORY mydb.public.notebooks_repo TO ROLE github_actions_role;

* **Issue: Git repository not found**
* Solution:
  - Verify repo exists: SHOW GIT REPOSITORIES;
  - Check spelling of SNOWFLAKE_GIT_REPO secret

* **Issue: Notebook not found in Git repo**
* Solution:
  - List files: LS @mydb.public.notebooks_repo/branches/main/notebooks/;
  - Ensure path matches: @repo/branches/main/notebooks/file.ipynb

* **Issue: Warehouse suspended**
* Solution:
  - Add auto-resume: ALTER WAREHOUSE compute_wh SET AUTO_RESUME = TRUE;
  - Or resume manually: ALTER WAREHOUSE compute_wh RESUME;

## PART 4: ADVANCED CONFIGURATION

Optional: Multiple Branches
----------------------------
```yaml
# Execute from different branch
snow git execute "@mydb.public.notebooks_repo/branches/develop/notebooks/analysis.ipynb" -x

# In workflow, use GitHub context
run: |
  BRANCH=${GITHUB_REF##*/}
  snow git execute "@$SNOWFLAKE_GIT_REPO/branches/$BRANCH/notebooks/analysis.ipynb" -x

Optional: Parameterized Notebooks
----------------------------------
# Execute with parameters (if notebook supports them)
snow git execute "@mydb.public.notebooks_repo/branches/main/notebooks/analysis.ipynb" \
  --parameter "start_date=2024-01-01" \
  --parameter "end_date=2024-12-31" \
  -x
```

Optional: Scheduled Runs
-------------------------
```yaml
# Add to workflow to run on schedule
on:
  schedule:
    - cron: '0 2 * * *'  # Daily at 2 AM UTC
  push:
    branches: [main]
```

Optional: Environment-Specific Workflows
-----------------------------------------
```yaml
# Use different Git repos for dev/prod
jobs:
  test-dev:
    environment: development
    steps:
      - name: Execute notebooks
        env:
          SNOWFLAKE_GIT_REPO: mydb.dev.notebooks_repo
        run: snow git execute "@$SNOWFLAKE_GIT_REPO/branches/develop/notebooks/analysis.ipynb" -x
  
  test-prod:
    environment: production
    steps:
      - name: Execute notebooks
        env:
          SNOWFLAKE_GIT_REPO: mydb.prod.notebooks_repo
        run: snow git execute "@$SNOWFLAKE_GIT_REPO/branches/main/notebooks/analysis.ipynb" -x
```

## PART 5: VERIFICATION CHECKLIST

#### Snowflake Setup:
  * [ ] API integration created and enabled
  * [ ] GitHub secret created with PAT
  * [ ] Git repository created and fetched
  * [ ] Service account user created (TYPE = SERVICE)
  * [ ] Public key set on user
  * [ ] Role created with all necessary grants
  * [ ] Role assigned to user
  * [ ] Local authentication test successful

#### GitHub Setup:
  * [ ] All secrets added to repository
  * [ ] Workflow file created in .github/workflows/
  * [ ] Notebook names match in workflow and repository
  * [ ] Branch names correct in workflow

#### Testing:
  * [ ] Local CLI test successful
  * [ ] Workflow triggered successfully
  * [ ] All notebooks executed without errors
  * [ ] Workflow completes with green checkmark


## SUMMARY: KEY CONCEPTS

1. NATIVE INTEGRATION: Notebooks stay in GitHub, Snowflake reads them directly
2. SERVICE ACCOUNT: Dedicated user for automation (TYPE = SERVICE)
3. KEY PAIR AUTH: More secure than passwords, no secrets stored in code
4. GIT REPOSITORY: Snowflake object that mirrors your GitHub repo
5. WORKFLOW: Automated testing on every push to notebooks

#### Benefits:
* ✅ Single source of truth (GitHub)
* ✅ Full version control history
* ✅ Automatic testing on changes
* ✅ No manual deployment needed
* ✅ Secure authentication


## QUICK REFERENCE COMMANDS
```
# Sync Git repository
snow sql -q "ALTER GIT REPOSITORY mydb.public.notebooks_repo FETCH" -x

# List files in Git repository
snow sql -q "LS @mydb.public.notebooks_repo/branches/main/notebooks/" -x

# Execute notebook from Git
snow git execute "@mydb.public.notebooks_repo/branches/main/notebooks/analysis.ipynb" -x

# Show notebooks in schema
snow sql -q "SHOW NOTEBOOKS IN SCHEMA mydb.public" -x

# Test connection
snow connection test -x

# Check grants
snow sql -q "SHOW GRANTS TO ROLE github_actions_role" -x
```

## SUPPORT RESOURCES

* Snowflake Git Integration:
https://docs.snowflake.com/en/developer-guide/git/git-overview

* Snowflake CLI Reference:
https://docs.snowflake.com/en/developer-guide/snowflake-cli

* GitHub Actions:
https://docs.github.com/en/actions

* Snowflake CLI Action:
https://github.com/snowflakedb/snowflake-cli-action

