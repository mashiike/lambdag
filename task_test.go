package lambdag_test

import (
	"encoding/json"
	"testing"

	"github.com/mashiike/lambdag"
	"github.com/stretchr/testify/require"
)

func TestTaskResponseMarshalJSON(t *testing.T) {
	cases := []struct {
		name     string
		resp     *lambdag.TaskResponse
		expected string
	}{
		{
			name:     "empty",
			resp:     &lambdag.TaskResponse{},
			expected: "{}",
		},
		{
			name: "empty",
			resp: &lambdag.TaskResponse{
				Payload: json.RawMessage(`"hoge"`),
			},
			expected: `{"Payload":"hoge"}`,
		},
	}
	for _, c := range cases {
		t.Run(string(c.name), func(t *testing.T) {
			bs, err := json.MarshalIndent(c.resp, "", "  ")
			require.NoError(t, err)
			require.JSONEq(t, c.expected, string(bs))
		})
	}
}
