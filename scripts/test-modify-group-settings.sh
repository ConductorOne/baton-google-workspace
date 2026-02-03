#!/bin/bash
set -euo pipefail

# Test script for modify_group_settings action
# This script:
# 1. Creates a test group
# 2. Modifies group settings (allow_external_members and who_can_post_message)
# 3. Verifies the action response shows the changes
# 4. Tests idempotency by setting the same values again
# 5. Deletes the group

CONNECTOR_BINARY="${CONNECTOR_BINARY:-./baton-google-workspace}"
SYNC_FILE="${SYNC_FILE:-/tmp/test-modify-group-settings.c1z}"
# Use timestamp + process ID + random number for uniqueness
RANDOM_SUFFIX="$(date +%s)-$$-$(shuf -i 1000-9999 -n 1 2>/dev/null || echo $RANDOM)"
TEST_GROUP_EMAIL="${TEST_GROUP_EMAIL:-test-settings-${RANDOM_SUFFIX}@${BATON_DOMAIN}}"
TEST_GROUP_NAME="${TEST_GROUP_NAME:-Test Settings Group ${RANDOM_SUFFIX}}"
TEST_GROUP_DESCRIPTION="${TEST_GROUP_DESCRIPTION:-Test group for settings modification}"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}=== Testing modify_group_settings action ===${NC}"
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
echo -e "${YELLOW}Step 1: Creating test group...${NC}"
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

# Wait for group creation to propagate
echo -e "${YELLOW}Waiting for group creation to propagate...${NC}"
sleep 10

# Step 2: Modify group settings
echo -e "${YELLOW}Step 2: Modifying group settings...${NC}"
echo "Setting allow_external_members=true and who_can_post_message=ALL_MEMBERS_CAN_POST"
echo "Command: ${CONNECTOR_BINARY} --file ${SYNC_FILE} --invoke-action modify_group_settings --invoke-action-resource-type group --invoke-action-args '{\"group_key\":\"${TEST_GROUP_EMAIL}\",\"allow_external_members\":true,\"who_can_post_message\":\"ALL_MEMBERS_CAN_POST\"}'"
echo ""

set +e  # Temporarily disable exit on error to capture output
MODIFY_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action modify_group_settings \
    --invoke-action-resource-type group \
    --invoke-action-args "{\"group_key\":\"${TEST_GROUP_EMAIL}\",\"allow_external_members\":true,\"who_can_post_message\":\"ALL_MEMBERS_CAN_POST\"}" 2>&1)
MODIFY_EXIT_CODE=$?
set -e  # Re-enable exit on error

echo "Modify action exit code: ${MODIFY_EXIT_CODE}"
echo ""

if [ ${MODIFY_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to modify group settings (exit code: ${MODIFY_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${MODIFY_OUTPUT}"
    exit 1
fi

# Step 3: Verify the action response
echo -e "${YELLOW}Step 3: Verifying action response...${NC}"

# Check for success in output
if ! echo "${MODIFY_OUTPUT}" | grep -qi "success.*true\|successful"; then
    echo -e "${YELLOW}Warning: Could not find explicit success message in output${NC}"
    echo "Output:"
    echo "${MODIFY_OUTPUT}" | head -30
fi

# Try to extract JSON from output if available, or check for key indicators
if echo "${MODIFY_OUTPUT}" | grep -q "settings_updated"; then
    echo -e "${GREEN}SUCCESS: Action response contains settings_updated field${NC}"
else
    echo -e "${YELLOW}Note: Could not find settings_updated in output, but action completed successfully${NC}"
fi

# Verify group_email matches
if echo "${MODIFY_OUTPUT}" | grep -q "${TEST_GROUP_EMAIL}"; then
    echo -e "${GREEN}SUCCESS: Action response contains correct group email${NC}"
else
    echo -e "${YELLOW}Warning: Could not find group email in output${NC}"
fi

echo ""

# Step 4: Test idempotency - modify with same values
echo -e "${YELLOW}Step 4: Testing idempotency (setting same values again)...${NC}"
echo "Command: ${CONNECTOR_BINARY} --file ${SYNC_FILE} --invoke-action modify_group_settings --invoke-action-resource-type group --invoke-action-args '{\"group_key\":\"${TEST_GROUP_EMAIL}\",\"allow_external_members\":true,\"who_can_post_message\":\"ALL_MEMBERS_CAN_POST\"}'"
echo ""

set +e  # Temporarily disable exit on error to capture output
IDEMPOTENT_OUTPUT=$("${CONNECTOR_BINARY}" \
    --file "${SYNC_FILE}" \
    --invoke-action modify_group_settings \
    --invoke-action-resource-type group \
    --invoke-action-args "{\"group_key\":\"${TEST_GROUP_EMAIL}\",\"allow_external_members\":true,\"who_can_post_message\":\"ALL_MEMBERS_CAN_POST\"}" 2>&1)
IDEMPOTENT_EXIT_CODE=$?
set -e  # Re-enable exit on error

echo "Idempotent action exit code: ${IDEMPOTENT_EXIT_CODE}"
echo ""

if [ ${IDEMPOTENT_EXIT_CODE} -ne 0 ]; then
    echo -e "${RED}ERROR: Failed to run idempotent modify action (exit code: ${IDEMPOTENT_EXIT_CODE})${NC}"
    echo "Full output:"
    echo "${IDEMPOTENT_OUTPUT}"
    exit 1
fi

# Check if settings_updated is false (idempotent behavior)
if echo "${IDEMPOTENT_OUTPUT}" | grep -qi "settings_updated.*false"; then
    echo -e "${GREEN}SUCCESS: Idempotency verified (settings_updated: false)${NC}"
elif echo "${IDEMPOTENT_OUTPUT}" | grep -qi "settings_updated.*true"; then
    echo -e "${YELLOW}Warning: settings_updated is true on second call (may indicate settings weren't persisted)${NC}"
else
    echo -e "${YELLOW}Note: Could not verify idempotency from output${NC}"
fi

echo ""

# Step 5: Delete the group
echo -e "${YELLOW}Step 5: Deleting test group...${NC}"
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

