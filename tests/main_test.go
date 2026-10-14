package tests

import (
	"os"
	"testing"

	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/amqptest"
	"github.com/uttamsingh23/tenant-control-plane/internal/testsupport/pgtest"
)

func TestMain(m *testing.M) {
	code := m.Run()
	pgtest.Teardown()
	amqptest.Teardown()
	os.Exit(code)
}
