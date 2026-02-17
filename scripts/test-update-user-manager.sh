#!/bin/bash
set -euo pipefail

# Test script for update_user_manager action
# This script:
# 1. Creates a test user
# 2. Invokes the update_user_manager action with a manager email
# 3. Syncs and verifies the user's manager_email in the profile
# 4. Deletes the test user

CONNECTOR_BINARY="${CONNECTOR_BINARY:-./baton-google-workspace}"
SYNC_FILE="${SYNC_FILE:-/tmp/test-update-user-manager.c1z}"
RANDOM_SUFFIX="$(date +%s)-$$-$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $RANDOM)"
TEST_USER_EMAIL="${TEST_USER_EMAIL:-test-manager-update-${RANDOM_SUFFIX}@${BATON_DOMAIN}}"
TEST_USER_GIVEN_NAME="${TEST_USER_GIVEN_NAME:-Test}"
TEST_USER_FAMILY_NAME="${TEST_USER_FAMILY_NAME:-ManagerUpdate}"
TEST_MANAGER_EMAIL="test-manager@baton.com"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Testing update_user_manager action ===${NC}"
echo "Test User Email: ${TEST_USER_EMAIL}"
echo "Target Manager Email: ${TEST_MANAGER_EMAIL}"
echo ""

# Cleanup function
cleanup() {
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo -e "\n${RED}Script failed with exit code ${EXIT_CODE}${NC}"
        echo -e "${YELLOW}Cleaning up (attempting to delete test user)...${NC}"
        if [ -n "${CREATED_USER_ID:-}" ]; then
            echo "Attempting to delete test user ${CREATED_USER_ID}..."
            "${CONNECTOR_BINARY}" \
                --file "${SYNC_FILE}" \
                --delete-resource "${CREATED_USER_ID}" \
                --delete-resource-type user 2>/dev/null || true
        fi
        echo "Sync file preserved at ${SYNC_FILE} for debugging"
    else
        if [ -f "${SYNC_FILE}" ]; then
            rm -f "${SYNC_FILE}"
        fi
    fi
}
trap cleanup EXIT

# Step 1: Create the test user
echo -e "${YELLOW}Step 1: Creating test user...${NC}"

set +e
CREATE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --create-account-email "${TEST_USER_EMAIL}" \
    --create-account-login "${TEST_USER_EMAIL}" \
    --create-account-profile "{\"email\":\"${TEST_USER_EMAIL}\",\"given_name\":\"${TEST_USER_GIVEN_NAME}\",\"family_name\":\"${TEST_USER_FAMILY_NAME}\"}" \
    --create-account-resource-type user 2>&1)
CREATE_EXIT_CODE=$?
set -e

echo "Create account exit code: ${CREATE_EXIT_CODE}"
echo ""

if [ ${CREATE_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to create user (exit code: ${CREATE_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${CREATE_OUTPUT}"
    exit 1
fi

# Wait for user creation to propagate
echo -e "${YELLOW}Waiting for user creation to propagate...${NC}"
sleep 40

# Step 2: Sync to get the created user ID
echo -e "${YELLOW}Step 2: Syncing to get created user ID...${NC}"
"${CONNECTOR_BINARY}" --file "${SYNC_FILE}"

# Extract user ID from sync file
echo "Finding created user ID..."
CREATED_USER_ID=$(baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
    jq -r --arg email "${TEST_USER_EMAIL}" \
    '.resources[] | select(.resource.annotations[]? | select(.["@type"] | contains("UserTrait")) | .emails[]? | .address == $email) | .resource.id.resource' | \
    head -1)

if [ -z "${CREATED_USER_ID}" ] || [ "${CREATED_USER_ID}" = "null" ]; then
    echo -e "${RED}ERROR: Could not find created user with email ${TEST_USER_EMAIL}${NC}"
    echo "Available users:"
    baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
        jq -r '.resources[] | select(.resource.annotations[]? | select(.["@type"] | contains("UserTrait"))) | {email: .resource.annotations[]? | select(.["@type"] | contains("UserTrait")) | .emails[0].address, id: .resource.id.resource}' || true
    exit 1
fi

echo -e "${GREEN}Found created user ID: ${CREATED_USER_ID}${NC}"
echo ""

# Step 3: Invoke the update_user_manager action
echo -e "${YELLOW}Step 3: Invoking update_user_manager action...${NC}"
echo "Setting manager to: ${TEST_MANAGER_EMAIL}"

set +e
ACTION_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action update_user_manager \
    --invoke-action-resource-type user \
    --invoke-action-args "{\"user_id\":\"${CREATED_USER_ID}\",\"manager_email\":\"${TEST_MANAGER_EMAIL}\"}" 2>&1)
ACTION_EXIT_CODE=$?
set -e

echo "Action exit code: ${ACTION_EXIT_CODE}"
echo ""

if [ ${ACTION_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to invoke update_user_manager action (exit code: ${ACTION_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${ACTION_OUTPUT}"
    exit 1
fi

# Wait for changes to propagate
echo -e "${YELLOW}Waiting for changes to propagate...${NC}"
sleep 40

# Step 4: Sync again to verify the manager was set
echo -e "${YELLOW}Step 4: Syncing to verify manager change...${NC}"
"${CONNECTOR_BINARY}" --file "${SYNC_FILE}"

# Verify the manager_email in the user profile
echo "Verifying manager_email..."
MANAGER_EMAIL=$(baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
    jq -r --arg user_id "${CREATED_USER_ID}" \
    '.resources[] | select(.resource.id.resource == $user_id) | .resource.annotations[] | select(.["@type"] == "type.googleapis.com/c1.connector.v2.UserTrait") | .profile.manager_email' | \
    head -1)

echo "Extracted manager_email value: '${MANAGER_EMAIL}'"

if [ -z "${MANAGER_EMAIL}" ] || [ "${MANAGER_EMAIL}" = "null" ]; then
    echo "Could not find manager_email field, checking user resource..."
    baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
        jq --arg user_id "${CREATED_USER_ID}" '.resources[] | select(.resource.id.resource == $user_id)' || true
    echo -e "${RED}ERROR: Could not extract manager_email field${NC}"
    exit 1
fi

if [ "${MANAGER_EMAIL}" != "${TEST_MANAGER_EMAIL}" ]; then
    echo -e "${RED}ERROR: Manager email mismatch. Expected: ${TEST_MANAGER_EMAIL}, Got: ${MANAGER_EMAIL}${NC}"
    exit 1
fi

echo -e "${GREEN}SUCCESS: Manager email verified (manager_email: ${MANAGER_EMAIL})${NC}"
echo ""

# Step 5: Delete the test user
echo -e "${YELLOW}Step 5: Deleting test user...${NC}"
if ! DELETE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --delete-resource "${CREATED_USER_ID}" \
    --delete-resource-type user 2>&1); then
    echo -e "${RED}ERROR: Failed to delete user${NC}"
    echo "${DELETE_OUTPUT}"
    exit 1
fi

echo -e "${GREEN}SUCCESS: User deleted successfully${NC}"
echo ""
echo -e "${GREEN}=== All tests passed! ===${NC}"

# Clear the user ID so cleanup doesn't try again
CREATED_USER_ID=""
