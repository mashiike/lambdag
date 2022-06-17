package lambdag_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	lambdasdk "github.com/aws/aws-sdk-go-v2/service/lambda"
	"github.com/mashiike/lambdag"
	"github.com/stretchr/testify/require"
)

func TestStubInvokeSuccess(t *testing.T) {
	type TestResponse struct {
		Name    string
		Success bool
	}
	expectedPayload := map[string]interface{}{
		"Name":    "Sample",
		"Success": true,
	}
	mux := lambdag.NewLambdaAPIStubMux("HelloWorldFunction", lambda.NewHandler(func(payload json.RawMessage) (*TestResponse, error) {
		var p interface{}
		err := json.Unmarshal(payload, &p)
		t.Logf("payload: %#v", payload)
		require.NoError(t, err)
		require.EqualValues(t, expectedPayload, p)
		return &TestResponse{
			Name:    "name",
			Success: true,
		}, nil
	}))
	server := httptest.NewServer(mux)
	cfg := aws.NewConfig()
	client := lambdasdk.NewFromConfig(*cfg, func(opts *lambdasdk.Options) {
		opts.EndpointResolver = lambdasdk.EndpointResolverFunc(func(_ string, _ lambdasdk.EndpointResolverOptions) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           server.URL,
				SigningRegion: "us-east-1",
			}, nil
		})
	})
	output, err := client.Invoke(context.Background(), &lambdasdk.InvokeInput{
		FunctionName: aws.String("HelloWorldFunction"),
		Payload:      Must(json.Marshal(expectedPayload)),
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"Name":"name", "Success": true}`, string(output.Payload))
}

func TestStubInvokeError(t *testing.T) {
	type TestResponse struct {
		Name    string
		Success bool
	}
	expectedPayload := map[string]interface{}{
		"Name":    "Sample",
		"Success": true,
	}
	mux := lambdag.NewLambdaAPIStubMux("HelloWorldFunction", lambda.NewHandler(func(payload json.RawMessage) (*TestResponse, error) {
		var p interface{}
		err := json.Unmarshal(payload, &p)
		t.Logf("payload: %#v", payload)
		require.NoError(t, err)
		require.EqualValues(t, expectedPayload, p)
		return nil, errors.New("test error")
	}))
	server := httptest.NewServer(mux)
	cfg := aws.NewConfig()
	client := lambdasdk.NewFromConfig(*cfg, func(opts *lambdasdk.Options) {
		opts.EndpointResolver = lambdasdk.EndpointResolverFunc(func(_ string, _ lambdasdk.EndpointResolverOptions) (aws.Endpoint, error) {
			return aws.Endpoint{
				URL:           server.URL,
				SigningRegion: "us-east-1",
			}, nil
		})
	})
	output, err := client.Invoke(context.Background(), &lambdasdk.InvokeInput{
		FunctionName: aws.String("HelloWorldFunction"),
		Payload:      Must(json.Marshal(expectedPayload)),
	})
	require.NoError(t, err)
	require.JSONEq(t, `{"errorMessage":"test error", "errorType":"errorString"}`, string(output.Payload))
	require.EqualValues(t, "errorString", *output.FunctionError)
}

func Must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}
