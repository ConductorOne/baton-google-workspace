#!/bin/bash
set -euo pipefail

# Test script for create_group action
# This script:
# 1. Creates a group using the create_group action
# 2. Attempts to create the same group again (should get 409)
# 3. Deletes the group using the email address

CONNECTOR_BINARY="${CONNECTOR_BINARY:-./baton-google-workspace}"
SYNC_FILE="${SYNC_FILE:-/tmp/test-create-group-sync.c1z}"
# Use timestamp + process ID + random number for uniqueness
# This ensures uniqueness even if multiple runs happen in the same second
RANDOM_SUFFIX="$(date +%s)-$$-$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $RANDOM)"
TEST_GROUP_EMAIL="${TEST_GROUP_EMAIL:-test-group-${RANDOM_SUFFIX}@${BATON_DOMAIN}}"
TEST_GROUP_NAME="${TEST_GROUP_NAME:-Test Group ${RANDOM_SUFFIX}}"
TEST_GROUP_DESCRIPTION="${TEST_GROUP_DESCRIPTION:-Test group created by CI}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Testing create_group action ===${NC}"
echo "Test Group Email: ${TEST_GROUP_EMAIL}"
echo "Test Group Name: ${TEST_GROUP_NAME}"
echo ""

# Cleanup function
cleanup() {
    EXIT_CODE=$?
    if [ $EXIT_CODE -ne 0 ]; then
        echo -e "\n${RED}Script failed with exit code ${EXIT_CODE}${NC}"
        echo -e "${YELLOW}Cleaning up (attempting to delete test group)...${NC}"
        # Try to delete the group if test failed (using email)
        if [ -n "${TEST_GROUP_EMAIL:-}" ]; then
            echo "Attempting to delete test group ${TEST_GROUP_EMAIL}..."
            "${CONNECTOR_BINARY}" \
                --file "${SYNC_FILE}" \
                --delete-resource "${TEST_GROUP_EMAIL}" \
                --delete-resource-type group 2>/dev/null || true
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

# Step 1: Create the group using the create_group action
echo -e "${YELLOW}Step 1: Creating group via create_group action...${NC}"
echo "Command: ${CONNECTOR_BINARY} --file ${SYNC_FILE} --invoke-action create_group --invoke-action-resource-type group --invoke-action-args '{\"email\":\"${TEST_GROUP_EMAIL}\",\"name\":\"${TEST_GROUP_NAME}\",\"description\":\"${TEST_GROUP_DESCRIPTION}\"}'"
echo ""

set +e  # Temporarily disable exit on error to capture output
CREATE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action create_group \
    --invoke-action-resource-type group \
    --invoke-action-args "{\"email\":\"${TEST_GROUP_EMAIL}\",\"name\":\"${TEST_GROUP_NAME}\",\"description\":\"${TEST_GROUP_DESCRIPTION}\"}" 2>&1)
CREATE_EXIT_CODE=$?
set -e  # Re-enable exit on error

echo "Create action exit code: ${CREATE_EXIT_CODE}"
echo ""

if [ ${CREATE_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to create group (exit code: ${CREATE_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${CREATE_OUTPUT}"
    exit 1
fi

# Step 2: Verify group was created by attempting duplicate creation
# We don't need to extract the ID - we'll use the email for deletion
echo -e "${YELLOW}Step 2: Verifying group was created by attempting duplicate creation (should get 409)...${NC}"

set +e  # Temporarily disable exit on error to capture output
DUPLICATE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action create_group \
    --invoke-action-resource-type group \
    --invoke-action-args "{\"email\":\"${TEST_GROUP_EMAIL}\",\"name\":\"${TEST_GROUP_NAME}\",\"description\":\"${TEST_GROUP_DESCRIPTION}\"}" 2>&1)
DUPLICATE_EXIT_CODE=$?
set -e  # Re-enable exit on error

if [ ${DUPLICATE_EXIT_CODE} -eq 0 ]; then
    echo -e "${RED}ERROR: Expected 409 error on duplicate creation, but got success${NC}"
    echo "This suggests the group was not created properly."
    exit 1
fi

# Check if we got a 409 error
if echo "${DUPLICATE_OUTPUT}" | grep -q "Error 409.*Entity already exists\|duplicate"; then
    echo -e "${GREEN}SUCCESS: Got expected 409 error - group exists!${NC}"
else
    echo -e "${RED}ERROR: Expected 409 error, but got different error${NC}"
    echo "Duplicate creation output:"
    echo "${DUPLICATE_OUTPUT}" | head -20
    exit 1
fi
echo ""

# Step 3: Delete the group using the email address
echo -e "${YELLOW}Step 3: Deleting group (email: ${TEST_GROUP_EMAIL})...${NC}"
if ! DELETE_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --delete-resource "${TEST_GROUP_EMAIL}" \
    --delete-resource-type group 2>&1); then
    echo -e "${RED}ERROR: Failed to delete group${NC}"
    echo "${DELETE_OUTPUT}"
    exit 1
fi

echo -e "${GREEN}SUCCESS: Group deleted successfully${NC}"
echo ""
echo -e "${GREEN}=== All tests passed! ===${NC}"

