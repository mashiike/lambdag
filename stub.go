package lambdag

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"
	"reflect"
	"regexp"
	"strconv"
	"strings"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-lambda-go/lambda/messages"
	"github.com/aws/aws-lambda-go/lambdacontext"
	"github.com/aws/aws-sdk-go-v2/aws/arn"
	"github.com/google/uuid"
)

type LambdaAPIStubMux struct {
	functionName string
	handler      lambda.Handler
	reg          *regexp.Regexp
}

// https://docs.aws.amazon.com/ja_jp/lambda/latest/dg/API_Invoke.html
const functionNameRegexpPattern = `(arn:(aws[a-zA-Z-]*)?:lambda:)?([a-z]{2}(-gov)?-[a-z]+-\d{1}:)?(\d{12}:)?(function:)?([a-zA-Z0-9-_\.]+)(:(\$LATEST|[a-zA-Z0-9-_]+))?`

func NewLambdaAPIStubMux(functionName string, handler lambda.Handler) *LambdaAPIStubMux {
	return &LambdaAPIStubMux{
		functionName: functionName,
		handler:      handler,
		reg:          regexp.MustCompile(functionNameRegexpPattern),
	}
}

func (mux *LambdaAPIStubMux) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	defer r.Body.Close()
	if r.Method != http.MethodPost {
		w.WriteHeader(http.StatusMethodNotAllowed)
		io.WriteString(w, http.StatusText(http.StatusMethodNotAllowed))
		return
	}
	pathParts := strings.Split(strings.TrimLeft(r.URL.Path, "/"), "/")
	if !strings.HasPrefix(r.URL.Path, "/2015-03-31/functions/") || len(pathParts) != 4 || !strings.HasSuffix(r.URL.Path, "/invocations") {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, http.StatusText(http.StatusNotFound))
		return
	}
	functionName := pathParts[2]
	if !mux.reg.MatchString(functionName) {
		w.Header().Set("X-Amzn-ErrorType", "InvalidParameterValueException")
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "FunctionName is invalid")
		return
	}
	qualifier := r.URL.Query().Get("Qualifier")

	functionARN, err := arn.Parse(functionName)
	if err != nil {
		functionARN = arn.ARN{
			Partition: "aws",
			AccountID: "123456789012",
			Service:   "lambda",
			Region:    os.Getenv("AWS_DEFAULT_REGION"),
			Resource:  fmt.Sprintf("function:%s", functionName),
		}
		if functionARN.Region == "" {
			functionARN.Region = "us-east-1"
		}
		if qualifier != "" {
			functionARN.Resource += ":" + qualifier
		}
	} else {
		functionName = strings.TrimPrefix(functionARN.Resource, "function:")
	}

	if functionName != mux.functionName {
		w.Header().Set("X-Amzn-ErrorType", "ResourceNotFoundException")
		w.WriteHeader(http.StatusNotFound)
		fmt.Fprintf(w, "Function not found: %s", functionARN.String())
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		w.Header().Set("X-Amzn-ErrorType", "InvalidRequestContentException")
		w.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(w, "can not read payload: %s", err.Error())
		return
	}

	if qualifier == "$LATEST" {
		w.Header().Set("X-Amz-Executed-Version", qualifier)
	} else if version, err := strconv.ParseUint(qualifier, 10, 64); err == nil {
		w.Header().Set("X-Amz-Executed-Version", fmt.Sprintf("%d", version))
	} else {
		w.Header().Set("X-Amz-Executed-Version", "1")
		if qualifier == "" {
			qualifier = "$LATEST"
		}
	}
	var logBuf bytes.Buffer
	uuidObj, _ := uuid.NewRandom()
	reqID := uuidObj.String()
	now := flextime.Now()
	defaultWriter := log.Default().Writer()
	logWriter := io.MultiWriter(defaultWriter, &logBuf)
	defer func() {
		log.Default().SetOutput(defaultWriter)
	}()
	fmt.Fprintf(logWriter, "%s START RequestId: %s Version: %s\n", now.Format("2006/01/02 15:04:05"), reqID, qualifier)
	invocationType := r.Header.Get("X-Amz-Invocation-Type")
	if invocationType == "" {
		invocationType = "RequestResponse"
	}
	var cc lambdacontext.ClientContext
	if base64ClientContext := r.Header.Get("X-Amz-Client-Context"); base64ClientContext != "" {
		decoder := json.NewDecoder(base64.NewDecoder(base64.StdEncoding, strings.NewReader(base64ClientContext)))
		decoder.Decode(&cc)
	}
	ctx := lambdacontext.NewContext(r.Context(), &lambdacontext.LambdaContext{
		AwsRequestID:       reqID,
		InvokedFunctionArn: functionARN.String(),
		ClientContext:      cc,
	})
	fmt.Fprintf(logWriter, "%s %s\n", now.Format("2006/01/02 15:04:05"), string(payload))
	output, err := mux.handler.Invoke(ctx, payload)
	endTime := flextime.Now()
	if err != nil {
		var ive messages.InvokeResponse_Error
		if errors.As(err, &ive) {
			w.Header().Set("X-Amz-Function-Error", ive.Type)
		} else {
			if errorType := reflect.TypeOf(err); errorType.Kind() == reflect.Ptr {
				ive.Type = errorType.Elem().Name()
			} else {
				ive.Type = errorType.Name()
			}
			w.Header().Set("X-Amz-Function-Error", ive.Type)
			ive.Message = err.Error()
		}
		bs, _ := json.Marshal(ive)
		fmt.Fprintf(logWriter, "%s %s\n", endTime.Format("2006/01/02 15:04:05"), string(bs))
		output = bs
	} else {
		fmt.Fprintf(logWriter, "%s %s\n", endTime.Format("2006/01/02 15:04:05"), string(output))
	}
	fmt.Fprintf(logWriter, "%s END RequestId: %s\n", endTime.Format("2006/01/02 15:04:05"), reqID)
	fmt.Fprintf(logWriter, "%s REPORT RequestId: %s  Init Duration x.xx ms Duration:%02f ms     Billed Duration yyy ms Memory Size 128 MB     Max Memory Used: 128 MB\n", endTime.Format("2006/01/02 15:04:05"), reqID, float64(endTime.Sub(now).Microseconds())/1000.0)

	if r.Header.Get("X-Amz-Log-Type") == "Tail" && invocationType == "RequestResponse" {
		var buf bytes.Buffer
		enc := base64.NewEncoder(base64.RawStdEncoding, &buf)
		enc.Write(logBuf.Bytes())
		enc.Close()
		w.Header().Set("X-Amz-Log-Result", buf.String())
	}
	w.WriteHeader(http.StatusOK)
	w.Write(output)
}
