#!/bin/bash
set -euo pipefail

# Test script for offboarding_profile_update action
# This script:
# 1. Creates a test user
# 2. Invokes the offboarding_profile_update action (without archiving)
# 3. Verifies the action succeeds and user is removed from GAL
# 4. Deletes the test user

CONNECTOR_BINARY="${CONNECTOR_BINARY:-./baton-google-workspace}"
SYNC_FILE="${SYNC_FILE:-/tmp/test-offboarding-profile-update.c1z}"
# Use timestamp + process ID + random number for uniqueness
RANDOM_SUFFIX="$(date +%s)-$$-$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $RANDOM)"
TEST_USER_EMAIL="${TEST_USER_EMAIL:-test-offboarding-${RANDOM_SUFFIX}@${BATON_DOMAIN}}"
TEST_USER_GIVEN_NAME="${TEST_USER_GIVEN_NAME:-Test}"
TEST_USER_FAMILY_NAME="${TEST_USER_FAMILY_NAME:-Offboarding}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Testing offboarding_profile_update action ===${NC}"
echo "Test User Email: ${TEST_USER_EMAIL}"
echo ""

# Cleanup function
cleanup() {
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo -e "\n${RED}Script failed with exit code ${EXIT_CODE}${NC}"
        echo -e "${YELLOW}Cleaning up (attempting to delete test user)...${NC}"
        # Try to delete the user if test failed
        if [ -n "${CREATED_USER_ID:-}" ]; then
            echo "Attempting to delete test user ${CREATED_USER_ID}..."
            "${CONNECTOR_BINARY}" \
                --file "${SYNC_FILE}" \
                --delete-resource "${CREATED_USER_ID}" \
                --delete-resource-type user 2>/dev/null || true
        fi
        echo "Sync file preserved at ${SYNC_FILE} for debugging"
    else
        # Test passed, just clean up the sync file
        if [ -f "${SYNC_FILE}" ]; then
            rm -f "${SYNC_FILE}"
        fi
    fi
}
trap cleanup EXIT

# Step 1: Create the test user
echo -e "${YELLOW}Step 1: Creating test user...${NC}"
echo "Command: ${CONNECTOR_BINARY} --file ${SYNC_FILE} --create-account-email ${TEST_USER_EMAIL} --create-account-login ${TEST_USER_EMAIL} --create-account-profile '{\"email\":\"${TEST_USER_EMAIL}\",\"given_name\":\"${TEST_USER_GIVEN_NAME}\",\"family_name\":\"${TEST_USER_FAMILY_NAME}\"}' --create-account-resource-type user"
echo ""

set +e  # Temporarily disable exit on error to capture output
CREATE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --create-account-email "${TEST_USER_EMAIL}" \
    --create-account-login "${TEST_USER_EMAIL}" \
    --create-account-profile "{\"email\":\"${TEST_USER_EMAIL}\",\"given_name\":\"${TEST_USER_GIVEN_NAME}\",\"family_name\":\"${TEST_USER_FAMILY_NAME}\"}" \
    --create-account-resource-type user 2>&1)
CREATE_EXIT_CODE=$?
set -e  # Re-enable exit on error

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

# Fallback if jq fails or returns empty
if [ -z "${CREATED_USER_ID}" ] || [ "${CREATED_USER_ID}" = "null" ]; then
    echo "Attempting fallback method to extract user ID..."
    # Try using console output and grep
    RESOURCES_OUTPUT=$(baton resources -f "${SYNC_FILE}" -t user 2>&1 || true)
    CREATED_USER_ID=$(echo "${RESOURCES_OUTPUT}" | \
        grep -i "${TEST_USER_EMAIL}" | \
        head -1 | \
        sed -n 's/.*resource_id: "\([^"]*\)".*/\1/p' || true)
fi

if [ -z "${CREATED_USER_ID}" ] || [ "${CREATED_USER_ID}" = "null" ]; then
    echo -e "${RED}ERROR: Could not find created user with email ${TEST_USER_EMAIL}${NC}"
    echo "Available users:"
    baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
        jq -r '.resources[] | select(.resource.annotations[]? | select(.["@type"] | contains("UserTrait"))) | {email: .resource.annotations[]? | select(.["@type"] | contains("UserTrait")) | .emails[0].address, id: .resource.id.resource}' || true
    exit 1
fi

echo -e "${GREEN}✓ Found created user ID: ${CREATED_USER_ID}${NC}"
echo ""

# Step 3: Invoke the offboarding_profile_update action (without archiving)
echo -e "${YELLOW}Step 3: Invoking offboarding_profile_update action (without archiving)...${NC}"
echo "Command: ${CONNECTOR_BINARY} --file ${SYNC_FILE} --invoke-action offboarding_profile_update --invoke-action-resource-type user --invoke-action-args '{\"user_id\":\"${CREATED_USER_ID}\"}'"
echo ""

set +e  # Temporarily disable exit on error to capture output
ACTION_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action offboarding_profile_update \
    --invoke-action-resource-type user \
    --invoke-action-args "{\"user_id\":\"${CREATED_USER_ID}\"}" 2>&1)
ACTION_EXIT_CODE=$?
set -e  # Re-enable exit on error

echo "Action exit code: ${ACTION_EXIT_CODE}"
echo ""

if [ ${ACTION_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to invoke offboarding_profile_update action (exit code: ${ACTION_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${ACTION_OUTPUT}"
    exit 1
fi

# Wait for changes to propagate
echo -e "${YELLOW}Waiting for changes to propagate...${NC}"
sleep 40

# Step 4: Sync again to verify the changes
echo -e "${YELLOW}Step 4: Syncing to verify offboarding changes...${NC}"
"${CONNECTOR_BINARY}" --file "${SYNC_FILE}"

# Verify that IncludeInGlobalAddressList is false
echo "Verifying user was removed from GAL..."
INCLUDE_IN_GAL=$(baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
    jq -r --arg user_id "${CREATED_USER_ID}" \
    '.resources[] | select(.resource.id.resource == $user_id) | .resource.annotations[] | select(.["@type"] == "type.googleapis.com/c1.connector.v2.UserTrait") | .profile.include_in_global_address_list' | \
    head -1)

# Debug output
echo "Extracted include_in_global_address_list value: '${INCLUDE_IN_GAL}'"

if [ -z "${INCLUDE_IN_GAL}" ] || [ "${INCLUDE_IN_GAL}" = "null" ]; then
    # If not found, try to get the full user resource for debugging
    echo "Could not find include_in_global_address_list field, checking user resource..."
    baton resources -f "${SYNC_FILE}" -t user -o json 2>/dev/null | \
        jq --arg user_id "${CREATED_USER_ID}" '.resources[] | select(.resource.id.resource == $user_id)' || true
    echo -e "${RED}ERROR: Could not extract include_in_global_address_list field${NC}"
    exit 1
fi

# Convert boolean to string for comparison (jq returns true/false as strings)
if [ "${INCLUDE_IN_GAL}" = "true" ]; then
    echo -e "${RED}ERROR: User is still in Global Address List (include_in_global_address_list: ${INCLUDE_IN_GAL})${NC}"
    echo "Expected: false"
    exit 1
fi

echo -e "${GREEN}SUCCESS: User removed from GAL (include_in_global_address_list: ${INCLUDE_IN_GAL})${NC}"
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

