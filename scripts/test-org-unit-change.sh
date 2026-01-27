#!/bin/bash
set -e

# Test script for change_user_org_unit action with user creation/deletion
# This script tests the change_user_org_unit action by:
# 1. Creating a test user
# 2. Changing the user's org unit
# 3. Syncing and verifying the change
# 4. Deleting the test user

TEST_USER_EMAIL="${TEST_USER_EMAIL:-test-orgunit-$(date +%s)@${BATON_DOMAIN}}"
TEST_USER_GIVEN_NAME="${TEST_USER_GIVEN_NAME:-Test}"
TEST_USER_FAMILY_NAME="${TEST_USER_FAMILY_NAME:-User}"
TARGET_ORG_UNIT="${TARGET_ORG_UNIT}"
CONNECTOR_BINARY="${CONNECTOR_BINARY:-./baton-google-workspace}"
C1Z_FILE="${C1Z_FILE:-/tmp/org-unit-test.c1z}"

if [ -z "$TARGET_ORG_UNIT" ]; then
  echo "Skipping test: TARGET_ORG_UNIT not set"
  exit 0
fi

if [ -z "$BATON_DOMAIN" ]; then
  echo "ERROR: BATON_DOMAIN must be set"
  exit 1
fi

# Cleanup function to delete user if test fails
cleanup() {
  if [ -n "$CREATED_USER_ID" ]; then
    echo "Cleaning up: Deleting test user $CREATED_USER_ID..."
    "$CONNECTOR_BINARY" \
      --file "$C1Z_FILE" \
      --delete-resource "$CREATED_USER_ID" \
      --delete-resource-type user || true
  fi
}

trap cleanup EXIT

# Create test user
echo "Creating test user: $TEST_USER_EMAIL"
"$CONNECTOR_BINARY" \
  --file "$C1Z_FILE" \
  --create-account-email "$TEST_USER_EMAIL" \
  --create-account-login "$TEST_USER_EMAIL" \
  --create-account-profile "{\"email\":\"$TEST_USER_EMAIL\",\"given_name\":\"$TEST_USER_GIVEN_NAME\",\"family_name\":\"$TEST_USER_FAMILY_NAME\"}" \
  --create-account-resource-type user

# Wait for user creation to propagate
echo "Waiting for user creation to propagate..."
sleep 10

# Sync to get the created user
echo "Syncing to get created user..."
"$CONNECTOR_BINARY" --file "$C1Z_FILE"

# Get the user ID from the sync
echo "Finding created user ID..."
CREATED_USER_ID=$(baton resources -f "$C1Z_FILE" -t user -o json | jq -r --arg email "$TEST_USER_EMAIL" '.resources[] | select(.resource.annotations[]? | select(.["@type"] | contains("UserTrait")) | .emails[]? | .address == $email) | .resource.id.resource' | head -1)

if [ -z "$CREATED_USER_ID" ]; then
  echo "ERROR: Could not find created user with email $TEST_USER_EMAIL"
  baton resources -f "$C1Z_FILE" -t user -o json | jq '.resources[] | select(.resource.annotations[]? | select(.["@type"] | contains("UserTrait"))) | {email: .resource.annotations[]? | select(.["@type"] | contains("UserTrait")) | .emails[0].address, id: .resource.id.resource}'
  exit 1
fi

echo "✓ Found created user ID: $CREATED_USER_ID"

# Verify initial org unit (should be default "/")
echo "Verifying initial org unit..."
INITIAL_ORG_UNIT=$(baton resources -f "$C1Z_FILE" -t user -o json | jq -r --arg user_id "$CREATED_USER_ID" '.resources[] | select(.resource.id.resource == $user_id) | .resource.annotations[] | select(.["@type"] | contains("UserTrait")) | .profile.org_unit_path // ""' | head -1)
echo "Initial org unit: $INITIAL_ORG_UNIT"

# Invoke action to change org unit
echo "Invoking change_user_org_unit action to move user to $TARGET_ORG_UNIT..."
"$CONNECTOR_BINARY" \
  --file "$C1Z_FILE" \
  --invoke-action change_user_org_unit \
  --invoke-action-resource-type user \
  --invoke-action-args "{\"user_id\":\"$CREATED_USER_ID\",\"org_unit_path\":\"$TARGET_ORG_UNIT\"}"

# Wait for changes to propagate
echo "Waiting for changes to propagate in Google Workspace..."
sleep 30

# Sync again to verify the change
echo "Syncing to verify org unit change..."
"$CONNECTOR_BINARY" --file "$C1Z_FILE"

# Verify the change took place
echo "Verifying org unit change..."
CHANGED_ORG_UNIT=$(baton resources -f "$C1Z_FILE" -t user -o json | jq -r --arg user_id "$CREATED_USER_ID" '.resources[] | select(.resource.id.resource == $user_id) | .resource.annotations[] | select(.["@type"] | contains("UserTrait")) | .profile.org_unit_path // ""' | head -1)
if [ -z "$CHANGED_ORG_UNIT" ]; then
  echo "ERROR: Could not find org_unit_path for user $CREATED_USER_ID after change"
  baton resources -f "$C1Z_FILE" -t user -o json | jq --arg user_id "$CREATED_USER_ID" '.resources[] | select(.resource.id.resource == $user_id)'
  exit 1
fi
if [ "$CHANGED_ORG_UNIT" != "$TARGET_ORG_UNIT" ]; then
  echo "ERROR: Org unit change failed. Expected: $TARGET_ORG_UNIT, Got: $CHANGED_ORG_UNIT"
  exit 1
fi
echo "✓ Org unit change verified: $CHANGED_ORG_UNIT"

# Delete the test user
echo "Deleting test user..."
"$CONNECTOR_BINARY" \
  --file "$C1Z_FILE" \
  --delete-resource "$CREATED_USER_ID" \
  --delete-resource-type user

# Clear the user ID so cleanup doesn't try again
CREATED_USER_ID=""

echo "✓ All tests passed!"

