package connector

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/conductorone/baton-sdk/pkg/pagination"
	rs "github.com/conductorone/baton-sdk/pkg/types/resource"
	"github.com/conductorone/baton-sdk/pkg/types/sessions"
	directoryAdmin "google.golang.org/api/admin/directory/v1"
	reportsAdmin "google.golang.org/api/admin/reports/v1"

	gwclient "github.com/conductorone/baton-google-workspace/pkg/client"
)

// fakeSessionStore is a minimal in-memory sessions.SessionStore for tests, namespacing keys by
// the Prefix set via sessions.WithPrefix (the same option applicationResource/app_login.go use).
type fakeSessionStore struct {
	data map[string][]byte
}

func newFakeSessionStore() *fakeSessionStore {
	return &fakeSessionStore{data: map[string][]byte{}}
}

func (f *fakeSessionStore) applyOpts(ctx context.Context, opt []sessions.SessionStoreOption) (*sessions.SessionStoreBag, error) {
	bag := &sessions.SessionStoreBag{}
	for _, o := range opt {
		if err := o(ctx, bag); err != nil {
			return nil, err
		}
	}
	return bag, nil
}

func (f *fakeSessionStore) storageKey(bag *sessions.SessionStoreBag, key string) string {
	return bag.Prefix + "\x00" + key
}

func (f *fakeSessionStore) Get(ctx context.Context, key string, opt ...sessions.SessionStoreOption) ([]byte, bool, error) {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return nil, false, err
	}
	v, ok := f.data[f.storageKey(bag, key)]
	return v, ok, nil
}

func (f *fakeSessionStore) GetMany(ctx context.Context, keys []string, opt ...sessions.SessionStoreOption) (map[string][]byte, []string, error) {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return nil, nil, err
	}
	result := map[string][]byte{}
	var missing []string
	for _, k := range keys {
		if v, ok := f.data[f.storageKey(bag, k)]; ok {
			result[k] = v
		} else {
			missing = append(missing, k)
		}
	}
	return result, missing, nil
}

func (f *fakeSessionStore) Set(ctx context.Context, key string, value []byte, opt ...sessions.SessionStoreOption) error {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return err
	}
	f.data[f.storageKey(bag, key)] = value
	return nil
}

func (f *fakeSessionStore) SetMany(ctx context.Context, values map[string][]byte, opt ...sessions.SessionStoreOption) error {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return err
	}
	for k, v := range values {
		f.data[f.storageKey(bag, k)] = v
	}
	return nil
}

func (f *fakeSessionStore) Delete(ctx context.Context, key string, opt ...sessions.SessionStoreOption) error {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return err
	}
	delete(f.data, f.storageKey(bag, key))
	return nil
}

func (f *fakeSessionStore) Clear(ctx context.Context, opt ...sessions.SessionStoreOption) error {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return err
	}
	prefix := bag.Prefix + "\x00"
	for k := range f.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			delete(f.data, k)
		}
	}
	return nil
}

func (f *fakeSessionStore) GetAll(ctx context.Context, pageToken string, opt ...sessions.SessionStoreOption) (map[string][]byte, string, error) {
	bag, err := f.applyOpts(ctx, opt)
	if err != nil {
		return nil, "", err
	}
	prefix := bag.Prefix + "\x00"
	result := map[string][]byte{}
	for k, v := range f.data {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			result[k[len(prefix):]] = v
		}
	}
	return result, "", nil
}

var _ sessions.SessionStore = (*fakeSessionStore)(nil)

// TestApplicationResourceList_PaginatesWithoutDuplicatingApps drives applicationResource.List()
// across multiple calls (small user batches per call) and verifies: every call's NextPageToken
// feeds the next call, the walk terminates, each discovered app resource is returned exactly
// once even though many users reference the same OAuth app, and the always-present Google
// Workspace resource is emitted exactly once, on the final page.
func TestApplicationResourceList_PaginatesWithoutDuplicatingApps(t *testing.T) {
	const totalUsers = 30
	users := make([]*directoryAdmin.User, 0, totalUsers)
	for i := range totalUsers {
		users = append(users, &directoryAdmin.User{
			Id:           fmt.Sprintf("user-%d", i),
			PrimaryEmail: fmt.Sprintf("user-%d@example.com", i),
		})
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/admin/directory/v1/users", func(w http.ResponseWriter, r *http.Request) {
		start := 0
		if pt := r.URL.Query().Get("pageToken"); pt != "" {
			n, err := strconv.Atoi(pt)
			if err != nil {
				http.Error(w, "bad page token", http.StatusBadRequest)
				return
			}
			start = n
		}
		const pageSize = 9
		end := start + pageSize
		if end > len(users) {
			end = len(users)
		}
		resp := &directoryAdmin.Users{Users: users[start:end]}
		if end < len(users) {
			resp.NextPageToken = strconv.Itoa(end)
		}
		_ = json.NewEncoder(w).Encode(resp)
	})
	mux.HandleFunc("/admin/directory/v1/users/", func(w http.ResponseWriter, r *http.Request) {
		// Tokens.list (GET .../users/{userKey}/tokens): every user authorized the same OAuth app.
		_ = json.NewEncoder(w).Encode(&directoryAdmin.Tokens{
			Items: []*directoryAdmin.Token{{ClientId: "shared-client", DisplayText: "Shared App"}},
		})
	})
	mux.HandleFunc("/admin/reports/v1/activity/users/", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(&reportsAdmin.Activities{})
	})
	server := httptest.NewServer(mux)
	defer server.Close()

	dir := newTestDirectoryService(t, server.URL, server.Client())
	rep := newReportsServiceForTest(t, server.URL, server.Client())
	client := &gwclient.GoogleWorkspaceClient{UserService: dir, UserSecurityService: dir, ReportService: rep}

	ar := newApplicationResource(client, "customer", "")
	ss := newFakeSessionStore()

	seenAppIDs := map[string]int{}
	pageToken := ""
	iterations := 0
	const maxIterations = 1000
	for {
		iterations++
		if iterations > maxIterations {
			t.Fatalf("applicationResource.List did not terminate after %d pages (possible infinite loop)", maxIterations)
		}

		resources, results, err := ar.List(context.Background(), nil, rs.SyncOpAttrs{
			Session:   ss,
			PageToken: pagination.Token{Token: pageToken},
		})
		if err != nil {
			t.Fatalf("List (page %d): %v", iterations, err)
		}
		for _, r := range resources {
			seenAppIDs[r.Id.Resource]++
		}
		if results.NextPageToken == "" {
			break
		}
		pageToken = results.NextPageToken
	}

	if n := seenAppIDs["shared-client"]; n != 1 {
		t.Fatalf("expected the shared OAuth app to be returned exactly once across all pages, got %d", n)
	}
	if n := seenAppIDs[googleWorkspaceAppID]; n != 1 {
		t.Fatalf("expected the Google Workspace app to be returned exactly once, got %d", n)
	}
}
