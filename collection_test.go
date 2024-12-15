package pocketbase

import (
	"testing"
	"time"

	"github.com/pluja/pocketbase/migrations"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListAccess(t *testing.T) {
	type auth struct {
		email    string
		password string
	}
	tests := []struct {
		name       string
		admin      auth
		user       auth
		collection string
		wantResult bool
		wantErr    bool
	}{
		{
			name:       "With admin credentials - posts_admin",
			admin:      auth{email: migrations.AdminEmailPassword, password: migrations.AdminEmailPassword},
			collection: migrations.PostsAdmin,
			wantResult: true,
			wantErr:    false,
		},
		{
			name:       "Without credentials - posts_admin",
			collection: migrations.PostsAdmin,
			wantErr:    true,
		},
		{
			name:       "Without credentials - posts_public",
			collection: migrations.PostsPublic,
			wantResult: true,
			wantErr:    false,
		},
		{
			// For access rule @request.auth.id != ""
			// no error is returned, but empty result
			name:       "Without credentials - posts_user",
			collection: migrations.PostsUser,
			wantResult: false,
			wantErr:    false,
		},
		{
			name:       "With user credentials - posts_user",
			user:       auth{email: migrations.UserEmailPassword, password: migrations.UserEmailPassword},
			collection: migrations.PostsUser,
			wantResult: true,
			wantErr:    false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := NewClient(defaultURL).Collection(tt.collection)
			if tt.admin.email != "" {
				c = NewClient(defaultURL, WithAdminEmailPassword(tt.admin.email, tt.admin.password)).Collection(tt.collection)
			} else if tt.user.email != "" {
				c = NewClient(defaultURL, WithUserEmailPassword(tt.user.email, tt.user.password)).Collection(tt.collection)
			}
			r, err := c.List(ParamsList{})
			assert.Equal(t, tt.wantErr, err != nil, err)
			assert.Equal(t, tt.wantResult, r.TotalItems > 0)
		})
	}
}

func TestCollection_List(t *testing.T) {
	defaultClient := NewClient(defaultURL)

	tests := []struct {
		name       string
		client     *Client
		collection string
		params     ParamsList
		wantResult bool
		wantErr    bool
	}{
		{
			name:       "List with no params",
			client:     defaultClient,
			collection: migrations.PostsPublic,
			wantErr:    false,
			wantResult: true,
		},
		{
			name:       "List no results - query",
			client:     defaultClient,
			collection: migrations.PostsPublic,
			params: ParamsList{
				Filters: "field='some_random_value'",
			},
			wantErr:    false,
			wantResult: false,
		},
		{
			name:       "List no results - invalid query",
			client:     defaultClient,
			collection: migrations.PostsPublic,
			params: ParamsList{
				Filters: "field~~~some_random_value'",
			},
			wantErr:    true,
			wantResult: false,
		},
		{
			name:       "List invalid collection",
			client:     defaultClient,
			collection: "invalid_collection",
			wantErr:    true,
			wantResult: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection := Collection[RecordBase]{tt.client, tt.collection, defaultClient.url + "/api/collections/" + tt.collection}
			got, err := collection.List(tt.params)
			assert.Equal(t, tt.wantErr, err != nil, err)
			assert.Equal(t, tt.wantResult, got.TotalItems > 0)
		})
	}
}

type TestRecord struct {
	RecordBase
	Field string `json:"field"`
}

func TestCollection_Delete(t *testing.T) {
	client := NewClient(defaultURL)
	field := "value_" + time.Now().Format(time.StampMilli)
	collection := Collection[TestRecord]{client, migrations.PostsPublic, client.url + "/api/collections/" + "collectionname"}

	// delete non-existing item
	err := collection.Delete("non_existing_id")
	assert.Error(t, err)

	// create temporary item
	resultCreated, err := collection.Create(TestRecord{
		Field: field,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, resultCreated.ID)

	// confirm item exists
	resultList, err := collection.List(ParamsList{Filters: "id='" + resultCreated.ID + "'"})
	assert.NoError(t, err)
	assert.Len(t, resultList.Items, 1)

	// delete temporary item
	err = collection.Delete(resultCreated.ID)
	assert.NoError(t, err)

	// confirm item does not exist
	resultList, err = collection.List(ParamsList{Filters: "id='" + resultCreated.ID + "'"})
	assert.NoError(t, err)
	assert.Len(t, resultList.Items, 0)
}

func TestCollection_Update(t *testing.T) {
	client := NewClient(defaultURL)
	field := "value_" + time.Now().Format(time.StampMilli)
	collection := Collection[map[string]any]{client, migrations.PostsPublic, client.url + "/api/collections/collectionname"}

	// update non-existing item
	err := collection.Update("non_existing_id", map[string]any{
		"field": field,
	})
	assert.Error(t, err)

	// create temporary item
	resultCreated, err := collection.Create(map[string]any{
		"field": field,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, resultCreated.ID)

	// confirm item exists
	resultList, err := collection.List(ParamsList{Filters: "id='" + resultCreated.ID + "'"})
	assert.NoError(t, err)
	require.Len(t, resultList.Items, 1)
	assert.Equal(t, field, resultList.Items[0]["field"])

	// update temporary item
	err = collection.Update(resultCreated.ID, map[string]any{
		"field": field + "_updated",
	})
	assert.NoError(t, err)

	// confirm changes
	resultList, err = collection.List(ParamsList{Filters: "id='" + resultCreated.ID + "'"})
	assert.NoError(t, err)
	require.Len(t, resultList.Items, 1)
	assert.Equal(t, field+"_updated", resultList.Items[0]["field"])
}

func TestCollection_Create(t *testing.T) {
	defaultClient := NewClient(defaultURL)
	defaultBody := map[string]interface{}{
		"field": "value_" + time.Now().Format(time.StampMilli),
	}

	tests := []struct {
		name       string
		client     *Client
		collection string
		body       any
		wantErr    bool
		wantID     bool
	}{
		{
			name:       "Create with no body",
			client:     defaultClient,
			collection: migrations.PostsPublic,
			wantErr:    false,
			wantID:     true,
		},
		{
			name:       "Create with body",
			client:     defaultClient,
			collection: migrations.PostsPublic,
			body:       defaultBody,
			wantErr:    false,
			wantID:     true,
		},
		{
			name:       "Create invalid collections",
			client:     defaultClient,
			collection: "invalid_collection",
			body:       defaultBody,
			wantErr:    true,
		},
		{
			name:       "Create no auth",
			client:     defaultClient,
			collection: migrations.PostsUser,
			body:       defaultBody,
			wantErr:    true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			collection := Collection[any]{tt.client, tt.collection, defaultClient.url + "/api/collections/" + tt.collection}
			r, err := collection.Create(tt.body)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
			if tt.wantID {
				assert.NotEmpty(t, r.ID)
			} else {
				assert.Empty(t, r.ID)
			}
		})
	}
}

func TestCollection_One(t *testing.T) {
	collection := NewClient(defaultURL).Collection(migrations.PostsPublic)
	field := "value_" + time.Now().Format(time.StampMilli)

	// update non-existing item
	_, err := collection.One("non_existing_id")
	assert.Error(t, err)

	// create temporary item
	resultCreated, err := collection.Create(map[string]any{
		"field": field,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, resultCreated.ID)

	// confirm item exists
	item, err := collection.One(resultCreated.ID)
	assert.NoError(t, err)
	assert.Equal(t, field, item["field"])

	// update temporary item
	err = collection.Update(resultCreated.ID, map[string]any{
		"field": field + "_updated",
	})
	assert.NoError(t, err)

	// confirm changes
	item, err = collection.One(resultCreated.ID)
	assert.NoError(t, err)
	assert.Equal(t, field+"_updated", item["field"])
}

func TestClient_OneTo(t *testing.T) {
	client := NewClient(defaultURL).Collection(migrations.PostsPublic)
	field := "value_" + time.Now().Format(time.StampMilli)

	// Get non-existing item
	var nonExistingResult map[string]any
	err := client.OneTo("non_existing_id", &nonExistingResult)
	assert.Error(t, err)

	// Create temporary item
	resultCreated, err := client.Create(map[string]any{
		"field": field,
	})
	assert.NoError(t, err)
	assert.NotEmpty(t, resultCreated.ID)

	// Define a struct for the response
	type Post struct {
		ID    string `json:"id"`
		Field string `json:"field"`
	}

	// Get existing item
	var result Post
	err = client.OneTo(resultCreated.ID, &result)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, field, result.Field)

	// Clean up: Delete temporary item
	err = client.Delete(resultCreated.ID)
	assert.NoError(t, err)
}
