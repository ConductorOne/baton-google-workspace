package connector

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"
	datatransfer "google.golang.org/api/admin/datatransfer/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestTransferReadRequiresVerifiedCompletion(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*datatransfer.DataTransfer)
		code   codes.Code
	}{
		{"complete", func(*datatransfer.DataTransfer) {}, codes.OK},
		{"different saved ID", func(v *datatransfer.DataTransfer) { v.Id = "another-transfer" }, codes.FailedPrecondition},
		{"different owner", func(v *datatransfer.DataTransfer) { v.NewOwnerUserId = "other" }, codes.FailedPrecondition},
		{"failed application", func(v *datatransfer.DataTransfer) { v.ApplicationDataTransfers[0].ApplicationTransferStatus = "failed" }, codes.FailedPrecondition},
		{"missing application status", func(v *datatransfer.DataTransfer) { v.ApplicationDataTransfers[0].ApplicationTransferStatus = "" }, codes.FailedPrecondition},
		{"unknown overall status", func(v *datatransfer.DataTransfer) { v.OverallTransferStatusCode = "unrecognized" }, codes.FailedPrecondition},
		{"missing parameters", func(v *datatransfer.DataTransfer) { v.ApplicationDataTransfers[0].ApplicationTransferParams = nil }, codes.FailedPrecondition},
		{"duplicate application", func(v *datatransfer.DataTransfer) {
			v.ApplicationDataTransfers = append(v.ApplicationDataTransfers, v.ApplicationDataTransfers[0])
		}, codes.FailedPrecondition},
		{"nil application", func(v *datatransfer.DataTransfer) {
			v.ApplicationDataTransfers = append(v.ApplicationDataTransfers, nil)
		}, codes.DataLoss},
	} {
		t.Run(test.name, func(t *testing.T) {
			record := &datatransfer.DataTransfer{
				Id: "tr_x", OldOwnerUserId: "src", NewOwnerUserId: "dst", OverallTransferStatusCode: "completed",
				ApplicationDataTransfers: []*datatransfer.ApplicationDataTransfer{{
					ApplicationId: appIdGoogleDocsAndGoogleDrive, ApplicationTransferStatus: "completed",
					ApplicationTransferParams: []*datatransfer.ApplicationTransferParam{{Key: "PRIVACY_LEVEL", Value: []string{"SHARED", "PRIVATE"}}},
				}},
			}
			test.change(record)
			calls := 0
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls++
				require.Equal(t, http.MethodGet, r.Method, "a transfer status read must never mutate")
				w.Header().Set("Content-Type", "application/json")
				require.NoError(t, json.NewEncoder(w).Encode(record))
			}))
			defer server.Close()
			connector := newTestConnector()
			primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
			result, _, err := connector.getUserDataTransfer(t.Context(), getTransferArgs("55656082996", []string{"private", "shared"}, nil))
			require.Equal(t, test.code, status.Code(err))
			require.Equal(t, test.code == codes.OK, result.GetFields()[fieldCompleted].GetBoolValue())
			require.Equal(t, "src", result.GetFields()[argResourceID].GetStringValue())
			require.Equal(t, record.NewOwnerUserId, result.GetFields()[argTargetResourceID].GetStringValue())
			require.Equal(t, 1, calls)
		})
	}
}

func TestTransferReadRequiresExplicitDrivePrivacy(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls++; w.WriteHeader(http.StatusInternalServerError) }))
	defer server.Close()
	connector := newTestConnector()
	primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
	_, _, err := connector.getUserDataTransfer(t.Context(), getTransferArgs("55656082996", nil, nil))
	require.Equal(t, codes.InvalidArgument, status.Code(err))
	require.Zero(t, calls)
}

func TestCalendarRetainResourcesCanBeVerified(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodGet, r.Method)
		w.Header().Set("Content-Type", "application/json")
		require.NoError(t, json.NewEncoder(w).Encode(&datatransfer.DataTransfer{
			Id: "tr_x", OldOwnerUserId: "src", NewOwnerUserId: "dst", OverallTransferStatusCode: "completed",
			ApplicationDataTransfers: []*datatransfer.ApplicationDataTransfer{{ApplicationId: appIdGoogleCalendar, ApplicationTransferStatus: "completed"}},
		}))
	}))
	defer server.Close()
	connector := newTestConnector()
	primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
	result, _, err := connector.getUserDataTransfer(t.Context(), getTransferArgs("435070579839", nil, structpb.NewBoolValue(false)))
	require.NoError(t, err)
	require.True(t, result.GetFields()[fieldCompleted].GetBoolValue())
}

func TestTransferDiscoveryCannotInsertAfterTruncation(t *testing.T) {
	reads, writes := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.Method != http.MethodGet {
			writes++
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		reads++
		_, _ = w.Write([]byte(`{"nextPageToken":"repeating-token"}`))
	}))
	defer server.Close()
	connector := newTestConnector()
	primeServiceCache(connector, nil, newTestDataTransferService(t, server.URL, server.Client()))
	_, _, err := connector.transferUserDriveFiles(t.Context(), driveTransferArgs())
	require.Error(t, err)
	require.Zero(t, writes)
	require.LessOrEqual(t, reads, 10, "discovery must have a fixed bound")
}
