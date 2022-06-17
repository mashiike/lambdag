package lambdag

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/aws/aws-lambda-go/lambda"
	"github.com/google/subcommands"
)

func Run(args []string, dag *DAG) error {
	return RunWithContext(context.Background(), args, dag)
}

func RunWithContext(ctx context.Context, args []string, dag *DAG) error {
	if isLambda() {
		handler := NewLambdaHandler(dag)
		lambda.StartWithOptions(handler, lambda.WithContext(ctx))
		return nil
	} else {
		commander, fs := newCommander(args, dag)
		fs.Parse(args)
		switch commander.Execute(ctx) {
		case subcommands.ExitSuccess:
			return errors.New("execute failed")
		case subcommands.ExitUsageError:
			return errors.New("usage error")
		}
		return nil
	}
}

func isLambda() bool {
	if strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") || os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		return true
	}
	return false
}

func newCommander(args []string, dag *DAG) (*subcommands.Commander, *flag.FlagSet) {
	fs := flag.NewFlagSet(dag.ID(), flag.ContinueOnError)
	commander := subcommands.NewCommander(fs, dag.ID())
	commander.Register(commander.HelpCommand(), "")
	commander.Register(commander.FlagsCommand(), "")
	commander.Register(commander.CommandsCommand(), "")
	commander.Register(&serveCommand{dag: dag, commander: commander}, "")
	return commander, fs
}

type serveCommand struct {
	commander *subcommands.Commander
	dag       *DAG
	port      int
}

func (cmd *serveCommand) Name() string     { return "serve" }
func (cmd *serveCommand) Synopsis() string { return "start a stub server for the lambda Invoke API" }
func (cmd *serveCommand) SetFlags(fs *flag.FlagSet) {
	fs.IntVar(&cmd.port, "port", 3001, "stub server port")
}
func (cmd *serveCommand) Usage() string {
	return fmt.Sprintf(`serve [options]:
	Start a Stub server for the Lambda Invoke API for local development.

	For example, it can be started from the aws cli as follows

	aws lambda --endpoint http://localhost:3001 invoke --function-name %s output.txt

`, cmd.dag.ID())
}
func (cmd *serveCommand) Execute(ctx context.Context, fs *flag.FlagSet, _ ...interface{}) subcommands.ExitStatus {
	if fs.Arg(0) == "help" {
		cmd.commander.ExplainCommand(cmd.commander.Output, cmd)
		return subcommands.ExitSuccess
	}
	l := log.Default()
	address := fmt.Sprintf(":%d", cmd.port)
	l.Printf("[info] starting up with Stub lambda API http://%s", address)
	listener, err := net.Listen("tcp", address)
	if err != nil {
		l.Printf("[error] couldn't listen to %s: %s", address, err.Error())
	}
	srv := http.Server{Handler: NewLambdaAPIStubMux(cmd.dag.ID(), NewLambdaHandler(cmd.dag))}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		<-ctx.Done()
		l.Println("[info] shutting down stub Lambda API", address)
		srv.Shutdown(ctx)
	}()
	if err := srv.Serve(listener); err != nil {
		if err != http.ErrServerClosed {
			log.Fatal(err)
		}
		wg.Done()
	}
	wg.Wait()
	return subcommands.ExitSuccess
}
